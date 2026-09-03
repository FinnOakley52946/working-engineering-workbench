# Media Cleanup Architecture: Cloud Cron or Queue for Rate-Limited API Processing?

A media-retention cleanup should not keep a web request open while it scans assets and calls a rate-limited deletion API. **Short answer: use a scheduled trigger to create a cleanup run, then put deletion candidates on a durable queue when the run can outlive one invocation or must absorb API quota pressure; use cron alone only for small, bounded runs whose delay and replay cost are acceptable.** The queue usually buys lower recovery latency and controlled concurrency at the price of another stateful component. Cron usually buys lower operational cost at the price of coarse recovery and a larger retry unit.

This is an architecture decision, not a product contest. A timer says when evaluation begins. A queue says which unit of work is available. Neither proves that a media object was deleted exactly once, so the application still needs an auditable run record and an idempotency boundary around the external effect.

## Should cloud cron or a queue control API rate-limited batch processing?

For periodic media cleanup, cloud cron should control admission into the batch, while a queue should control execution when the workload is unbounded, bursty, or expensive to repeat. The useful decision axis is latency versus cost: ask how long an eligible asset may remain undeleted after the scheduled scan, then price the machinery required to meet that delay under retries and backlog. “Cheapest” and “easiest” are incomplete without that recovery target.

The invariants come first. Every cleanup run needs a stable run ID. Every candidate needs a stable operation key derived from the media object and the retention rule, rather than from a delivery attempt. Eligibility must be checked again immediately before deletion because a legal hold, restoration, or policy change may occur after discovery. Finally, the audit trail must distinguish discovered, claimed, attempted, confirmed, deferred, and exempt states; collapsing them into “done” makes reconciliation impossible.

The failure boundaries are equally concrete. The scheduled trigger can be delivered more than once, a scan can stop after recording only part of its candidate set, a worker can lose its claim, and the deletion API can accept an operation while the caller remains uncertain about the result. An exactly-once mindset does not mean assuming exactly-once transport. It means arranging durable state so that each ambiguous boundary has a deterministic next action.

For a media library, consider one concrete passage through the state machine. A retention rule identifies a source file after its derived renditions have expired, but discovery is not permission to delete: the scanner first writes the run, the policy version, and a candidate row in a transaction-sized page, then publishes only the candidate ID. A worker claims that ID and reloads the current object record. If an editor restored the asset or compliance attached a hold between the scan and the claim, the worker records an exemption and makes no API call. Otherwise, it checks the stable operation key, waits for quota, submits the deletion, and records the observed outcome. If the worker loses its claim before making the call, another delivery can repeat those checks. If the result is ambiguous after the call, the candidate moves to reconciliation rather than being declared successful or blindly repeated. The run can therefore contain confirmed, exempt, deferred, and reconciling candidates at the same time — an intentionally untidy operational truth that a single green cron invocation would conceal. This longer path matters because the dangerous duplicate is not a repeated message; it is a repeated external deletion without a durable account of why it was allowed, while the dangerous omission is an eligible object that silently fell between a completed scan page and an unpublished work item.

Keep that distinction sharp.

## Decision record: latency, cost, and the retry unit

| Design | Recovery and backlog latency | Cost and operational load | Failure and audit boundary | Suitable case |
| --- | --- | --- | --- | --- |
| One cron invocation processes the whole scan | Recovery waits for the next run or a manual replay | Fewest moving parts; repeated scans can redo substantial work | The run is the retry unit, so partial progress must be checkpointed | Small, bounded sets with a generous cleanup window |
| Cron creates persisted pages | A failed page can resume without rescanning earlier pages | Adds database state but no queue service | The page is the retry unit; leases and checkpoints need explicit rules | Moderate sets where scheduled polling meets the latency target |
| Cron enqueues candidates for workers | Workers can drain backlog continuously within the API quota | Adds queue operations, consumers, and dead-letter handling | The candidate is the retry unit; queue delivery is not the audit record | Variable sets, tighter recovery targets, or costly repeated scans |
| A continuously running poller claims database rows | Poll interval bounds admission delay | Pays for a resident process and database polling | The claimed row is both work index and audit anchor | Teams already operating workers and transactional claiming |

The table deliberately avoids a universal winner. A nightly cleanup with a 24-hour deletion objective may gain nothing from per-item queueing if the candidate count has a hard upper bound and the entire run is cheap to replay. A newsroom ingest system with volatile volume may need a queue even when the mean batch is small, because the tail determines how quickly storage and policy state converge.

Priority does not create quota. RabbitMQ documents priority queues, which can help urgent takedowns overtake ordinary retention work, but worker concurrency and admission still have to respect the deletion API. Likewise, Amazon SQS documents a visibility timeout that temporarily hides a received message while it is being processed; the consumer must choose a processing lease compatible with the work and remain safe when a message becomes available again. Those mechanisms affect ordering and redelivery. They don't replace application idempotency.

I'm not sure a single cost estimate remains useful across media workloads without three measurements: candidates per scan, deletion latency per object, and the provider's actual quota behavior. Your mileage may vary. Capture those distributions in a shadow run before fixing worker count or polling cadence, because an average conceals the backlog that drives both recovery time and queue retention.

## Critical path in Go

The worker below depends on generic ports rather than a vendor SDK. The repository owns leases and audit transitions; the limiter owns local admission; the remote client owns the API call. `Confirm` must be implemented so that the operation key is unique for a specific retention decision. That uniqueness constraint is the point at which an exactly-once intention becomes enforceable application state.

```go
package cleanup

import (
	"context"
	"errors"
	"time"
)

type Candidate struct {
	ID           string
	ObjectID     string
	OperationKey string
}

type Repository interface {
	Claim(ctx context.Context, candidateID string, lease time.Duration) (Candidate, error)
	Eligible(ctx context.Context, candidate Candidate) (bool, error)
	AlreadyConfirmed(ctx context.Context, operationKey string) (bool, error)
	Confirm(ctx context.Context, candidate Candidate) error
	Defer(ctx context.Context, candidate Candidate, nextAttempt time.Time) error
}

type DeletionAPI interface {
	Delete(ctx context.Context, objectID, operationKey string) error
}

type Limiter interface {
	Wait(ctx context.Context) error
}

var ErrRateLimited = errors.New("rate limited")

func Process(
	ctx context.Context,
	repo Repository,
	api DeletionAPI,
	limiter Limiter,
	candidateID string,
	now func() time.Time,
) error {
	candidate, err := repo.Claim(ctx, candidateID, 2*time.Minute)
	if err != nil {
		return err
	}

	eligible, err := repo.Eligible(ctx, candidate)
	if err != nil || !eligible {
		return err
	}

	confirmed, err := repo.AlreadyConfirmed(ctx, candidate.OperationKey)
	if err != nil || confirmed {
		return err
	}

	if err := limiter.Wait(ctx); err != nil {
		return err
	}
	if err := api.Delete(ctx, candidate.ObjectID, candidate.OperationKey); err != nil {
		if errors.Is(err, ErrRateLimited) {
			return repo.Defer(ctx, candidate, now().Add(time.Minute))
		}
		return err
	}

	return repo.Confirm(ctx, candidate)
}
```

The two durations are policy inputs in a real service, not universal settings. The claim lease must exceed an ordinary processing attempt while still allowing abandoned work to return; the defer interval should come from the upstream contract or response where available. A fixed value is shown only to keep the state transition visible. Tests should inject the clock and limiter, run duplicate candidate IDs concurrently, expire a claim, and prove that the repository's unique operation key prevents two confirmations.

There is a harder ambiguity after `Delete` succeeds remotely but before `Confirm` commits locally. No queue choice erases it. If the upstream API accepts an idempotency key, reuse the operation key and reconcile by that key; if it does not expose a way to determine the prior result, the system cannot honestly promise exactly-once external effect. Document that compliance limit, restrict automatic retries according to the consequence of a duplicate, and retain enough audit evidence for review.

Deployment should separate the scanner's permission to enumerate candidates from the worker's permission to delete objects. Observability should report oldest eligible candidate age, claim expirations, deferred attempts, confirmations, exemptions, and reconciliation backlog by run ID. A raw count of successful invocations is weak evidence: it says little about whether every eligible object reached a terminal policy state.

## Rejected option and when it is valid

The rejected default is cron-only fan-out: the timer scans, deletes, sleeps to obey the quota, and returns after the last object. The catch is that scan state, quota pacing, and external effects share one execution lifetime. Its retry unit tends to become the entire invocation unless checkpoints are designed with the same care that a queued design would require, while a slow upstream extends the open execution path that this media job was meant to avoid.

Still, cron-only is suitable when the maximum candidate count is enforced, the run fits comfortably inside its execution budget, repeated eligibility checks are inexpensive, and the retention objective tolerates waiting until the next scheduled attempt. Stick with persisted-page polling when per-item queue overhead is unjustified but whole-run replay is too coarse. Choose queued workers when backlog age must recover continuously or when rescanning dominates the operating cost.

Those are capability boundaries, not maturity rankings. A repository-oriented scheduled workflow can be convenient for maintenance tied to source control; an application scheduler is a cleaner boundary for production policy work; a managed queue reduces queue operations work; a self-hosted broker gives the team more direct control but also more operational ownership. Compliance can change the decision again: retention evidence may need to live in an immutable system of record with access controls and retention rules that are independent of scheduler logs and queue retention.

The final rule is compact. Let the timer establish a run, let durable state prove eligibility and progress, and add a queue only when the required recovery latency or retry granularity justifies its cost.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- https://www.rabbitmq.com/docs/priority
