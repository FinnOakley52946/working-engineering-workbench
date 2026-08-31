# Weekly Digest Backlog Recovery: Failed Job Retry, Rate Limit Dedup, and Idempotency

Pick a standard at-least-once queue with a dead-letter path, and keep the dedup decision in your own database instead of in the broker: for a weekly digest that has to survive a rate-limited email provider, that pairing is the one that recovers cleanly, and every option I compare below still leaves the same idempotency work on your side of the boundary. The deciding constraint is the length of the recovery window. A failed job in this workload doesn't come back in seconds — a provider throttle plus a human triage step can push the second attempt hours past the first, which is longer than any broker-side dedup window on offer.

Transport dedup is an optimization. Business identity is the contract.

The system I have in mind is a customer support platform that mails every active customer a weekly digest: open tickets, first-response times, anything still sitting in their court. One run enqueues one job per customer — call it 200,000 jobs on a Monday morning — and the send is metered by the provider's rate limit rather than by how many workers you happen to be running. Operational recovery is therefore the primary axis. When a run stops two thirds of the way through because the provider starts answering 429, the interesting question is not which broker has the nicer console; it is whether the remaining third can be finished without mailing the first two thirds a second time.

## The invariant that outlives every redelivery

Each digest has exactly one durable identity: the pair (customer_id, period_id). Not the message id, not the attempt counter, not the broker's internal sequence number, because all three describe transport attempts, and a transport attempt is precisely the thing that gets duplicated. In a ledger the equivalent discipline is unremarkable: you record the intent, you record the effect, and you keep both rows so that a reconciliation pass can answer "did this happen, and did it happen once?" months later. A digest deserves the same treatment, since an email is a side effect that no rollback can retract. The worker's order of operations follows from that: claim the (customer_id, period_id) row under a lease, hand the send to the provider with that same key attached, record the provider's message id and terminal status, and only then acknowledge the queue message. Acknowledge first and you delete the only outstanding instruction to finish the work while its effect is still unresolved; acknowledge last and you end up with a table a support lead can read a week later, which is what anyone investigating a duplicate-mail complaint actually needs.

Compliance pins the boundary in the same place. The CAN-SPAM rules require an opt-out to be honoured within 10 business days, so a retry has to re-read the suppression list at send time rather than trusting a snapshot taken when the run was enqueued. A redrive that replays a two-day-old payload can mail someone who unsubscribed yesterday. That is not a broker feature you can shop for; it is a decision about where the payload's authority lives.

## What should a retry queue guarantee for rate-limited jobs: dedup, idempotency, or both?

Both, at different scopes, and only one of the two is yours to guarantee. Broker dedup is time-boxed by design. SQS FIFO deduplicates on a content hash or an explicit MessageDeduplicationId for a five-minute interval, so inside that interval a duplicate publish is discarded and outside it the identical body is simply a new message. Cloud Tasks deduplicates on a caller-supplied task name, which puts the identity in the request rather than in a rolling window, although the guarantee expires a bounded period after the earlier task with that name finished rather than persisting forever. BullMQ assigns each job an id and ignores a second add with the same id while that job record is still retained in Redis, which means `removeOnComplete` quietly shortens the dedup window to whatever your retention policy is. Pub/Sub offers exactly-once delivery on pull subscriptions within the acknowledgement deadline: a redelivery guarantee, not a statement about your side effects.

Rate limiting splits along the same seam. Cloud Tasks and BullMQ each expose a queue-level dispatch ceiling, so the limiter sits beside the work; SQS has no consumer throughput limiter of its own, which pushes pacing into your worker pool or into a token bucket you maintain yourself. Neither arrangement excuses you from reading the provider's answer. A 429 carrying Retry-After is the authoritative signal, and RFC 9110 defines that header as either a delay in seconds or an HTTP-date, so a client that parses integers only will mishandle half the legal responses. Retrying a throttled send in a tight loop converts a downstream limit into a larger backlog.

Back off, then return the message with a delay.

## Comparing the transports against a run that must resume mid-flight

The honest summary is that these five differ less in what they guarantee than in what they make visible afterwards.

| Transport | Dedup identity | Rate limiting | Recovery path | What you still own |
| --- | --- | --- | --- | --- |
| SQS standard | none; at-least-once delivery | consumer pool or token bucket | dead-letter queue plus redrive to source | dedup key and an idempotent apply |
| SQS FIFO | content hash or MessageDeduplicationId, five-minute interval | ordering per message group, not throughput | dead-letter queue; a stuck group blocks behind the failure | identity that outlives the interval |
| Cloud Tasks | caller-supplied task name | dispatch rate and concurrency per queue | per-task retry config; the HTTP target returns 2xx to complete | suppression re-check and the ledger row |
| Redis with BullMQ | job id while the record is retained | limiter with max and duration | failed set plus a scripted retry | Redis persistence, failover, memory headroom |
| RabbitMQ | none; DLX routes rejected or expired messages | consumer prefetch | dead-letter exchange, then a replay consumer | at-least-once dead-lettering configuration |

What separates them on the recovery axis is how much of a half-finished run an operator can see, and how much operational surface you accept in exchange for that visibility. A managed queue with a dead-letter queue and a redrive command hands you a countable pile of failures and a one-command replay, which is about as much as an on-call engineer wants at 07:00 on the morning the digest is due. Redis-backed queues give richer introspection and a scriptable failed set, and the price is that you own persistence, failover and memory headroom for a workload whose peak lasts one hour a week. RabbitMQ's dead-letter exchange is the most explicit routing model of the group, and it is also the one whose semantics you must read carefully: the documentation flags that dead-lettering is at-most-once by default, with at-least-once dead-lettering available on quorum queues. Pub/Sub is not suitable as a per-customer pacer at all; it is a fan-out bus with dead-letter topics.

## The critical path, with the dedup key in the database

The uniqueness constraint is the whole design, so it belongs in schema rather than in a comment.

```sql
create table digest_send (
  customer_id  bigint      not null,
  period_id    text        not null,
  status       text        not null,          -- claimed | sent | suppressed
  lease_until  timestamptz,
  provider_id  text,
  attempts     int         not null default 0,
  updated_at   timestamptz not null default now(),
  primary key (customer_id, period_id)
);
```

The handler treats every delivery as a question about state, never as permission to append a new effect. A claim that cannot be won means some other attempt holds the lease, and the correct answer there is to leave the message for later rather than to skip the customer.

```go
package digest

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Throttled carries the provider's Retry-After so the caller can requeue with a
// delay instead of spinning against a rate limit.
type Throttled struct{ RetryAfter time.Duration }

func (t *Throttled) Error() string { return "provider throttled the send" }

// ErrLeaseHeld tells the consumer to leave the message for a later attempt.
var ErrLeaseHeld = errors.New("another attempt holds the lease")

type Job struct {
	CustomerID int64
	PeriodID   string // "2026-W07": one digest per customer per period, forever
}

// Claim is the outcome of an upsert on (CustomerID, PeriodID).
type Claim struct {
	Owned bool // this attempt may perform the send
	Done  bool // an earlier attempt already recorded a terminal status
}

type Store interface {
	// Claim inserts the row, or takes it over when its lease has expired.
	Claim(ctx context.Context, j Job, lease time.Duration) (Claim, error)
	Suppressed(ctx context.Context, customerID int64) (bool, error)
	MarkSent(ctx context.Context, j Job, providerID string) error
}

type Mailer interface {
	Send(ctx context.Context, j Job, idempotencyKey string) (providerID string, err error)
}

func Handle(ctx context.Context, s Store, m Mailer, j Job) error {
	if j.PeriodID == "" {
		return errors.New("a digest job without a period id has no identity")
	}
	c, err := s.Claim(ctx, j, 5*time.Minute)
	if err != nil {
		return err // no acknowledgement: the message becomes visible again
	}
	if c.Done {
		return nil // already reconciled; acknowledge and move on
	}
	if !c.Owned {
		return ErrLeaseHeld
	}
	// Opt-out state is authoritative at send time, not at enqueue time.
	skip, err := s.Suppressed(ctx, j.CustomerID)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	providerID, err := m.Send(ctx, j, dedupKey(j))
	var throttled *Throttled
	if errors.As(err, &throttled) {
		return err // requeue after throttled.RetryAfter; the claim row survives
	}
	if err != nil {
		return err
	}
	return s.MarkSent(ctx, j, providerID)
}

func dedupKey(j Job) string {
	return fmt.Sprintf("digest:%s:%d", j.PeriodID, j.CustomerID)
}
```

Two properties matter more than the code around them. The key handed to the provider is derived from business identity alone, with no attempt number in it, so the provider can collapse a duplicate request on its own side when it supports an idempotency key. And the acknowledgement is a function of the returned error, which keeps the queue in charge of timing and attempts while the database stays in charge of truth. The `attempts` column is not decoration either: a run that reports 340 rows stuck in `claimed` is telling you something quite different from a run with 340 rows in `sent` and a duplicate complaint, and only one of those needs a human.

## The option I rejected: replaying the whole run from the queue

The tempting shortcut is to make the queue the run's memory. Publish one message per run, let a worker walk the customer list, and on failure republish the message so the walk begins again. It's a smaller amount of code, and it fails in the exact scenario the design exists for: a partially finished walk has no cursor that a redrive can respect, the 256 KB message body limit stops you from carrying the customer list inline anyway, and SQS retention tops out at 14 days, so the run's history disappears at roughly the moment a reconciliation question arrives. Delay is capped at 15 minutes per message, which is well short of the multi-hour provider throttles that started this whole discussion.

That shape does earn its place when the work is genuinely a graph rather than a fan-out: a digest that waits on a nightly aggregation, then a rendering step, then a send, with compensation at each stage. A workflow engine such as Temporal models those dependencies explicitly and keeps the execution history that queues discard, and queues remain the wrong tool for a directed graph no matter how many dead-letter hops you add. Stick with BullMQ when Redis is already an owned and monitored dependency and the limiter and failed set genuinely fit your operators' habits; the catch is that you inherit persistence and failover for a spike that lasts an hour a week. RabbitMQ suits an estate that already runs it, provided the dead-letter path is configured deliberately rather than inherited from a tutorial.

Write down the invariant before the transport. If a stalled run can be resumed by asking the database which (customer, period) pairs are still unsent, the queue is only a dispatcher and most of these options will carry the load; if the run's sole memory is the queue, then no dedup window is long enough and no retry policy is safe. The transport is the reversible decision. The identity is not.

## References

- CAN-SPAM Act compliance guide (opt-out within 10 business days) — https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
- Amazon SQS FIFO queues — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html
- Google Cloud Tasks documentation — https://cloud.google.com/tasks/docs
- BullMQ documentation — https://docs.bullmq.io/
- RabbitMQ dead letter exchanges — https://www.rabbitmq.com/docs/dlx
- Google Cloud Pub/Sub overview — https://cloud.google.com/pubsub/docs/overview
- RFC 9110, Retry-After — https://www.rfc-editor.org/rfc/rfc9110#field.retry-after
- Idempotency-Key HTTP header field (Internet-Draft) — https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
