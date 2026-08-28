# Failure Accounting for Node.js Image Processing: Queue Retries, Webhooks, and DLQ Redrive

**Short answer:** make each image-processing job a durable state machine keyed by one stable operation ID, retry only failures that may change with time, use capped exponential backoff with jitter, and treat DLQ redrive as an audited batch write rather than a button that gives every failed job another try.

That answer starts with a constraint: queue delivery and business completion are different events. A worker can finish an image, lose its acknowledgement, and receive the same job again; it can also write the image and fail before sending the webhook. No retry count repairs a design that represents those states with a single `done` boolean. The useful guarantee is therefore not an abstract promise that code runs once, but evidence that each externally visible effect has a stable identity, a recorded outcome, and a replay rule.

## How should a Node.js background job queue retry failed image processing webhooks?

The Node.js producer should assign an operation ID when accepting the request, persist that ID with the source reference and requested transformations, and enqueue only a reference to the persisted job. The worker implementation may be in Node.js, Go, or another language; its correctness boundary is the state transition in durable storage, not the process runtime. The queue is responsible for delayed delivery, while the job ledger is responsible for deciding whether a delivery still has work to do.

This distinction produces three failure classes. A transient failure has a time-dependent cause and may be retried. A permanent failure is determined by the request itself and should move directly to quarantine. An ambiguous failure occurs after an external effect may have succeeded but before the worker recorded success; blindly retrying that case is unsafe unless the external operation accepts the same idempotency key or can be reconciled by reading its current state.

The rule is short.

Retry uncertainty only after resolving what already happened. For image processing, that usually means deterministic output identities: the same operation ID and variant name address the same output object, so a second delivery can inspect or replace that object according to an explicit policy. For webhook delivery, include the operation ID in the signed payload and maintain a separate notification step; image completion and subscriber acknowledgement are not one transaction, and pretending otherwise makes the audit trail lie.

An HMAC can authenticate a webhook message when sender and receiver share a secret. RFC 2104 specifies the keyed-hashing construction, but it does not define JSON canonicalization, timestamp tolerance, replay retention, key rotation, or the HTTP header names for an application protocol. Those details belong in the webhook contract. Sign a precisely defined byte sequence, compare tags without leaking timing information, and let receivers deduplicate on the operation ID rather than on a delivery-attempt ID.

The ledger should preserve transitions such as `accepted`, `rendered`, `notification_pending`, `completed`, and `quarantined`, along with an append-only attempt record. A mutable status gives operators the present answer; the attempt records explain how it became true. That separation matters during reconciliation, because a queue depth can show pressure but cannot prove which effect committed.

## Backoff is load control, not a correctness mechanism

Exponential backoff spaces repeated attempts, and jitter prevents a cohort of failures from returning on exactly the same schedule. Neither property decides whether a retry is valid. That decision must already have been made by the failure classifier and the ledger.

A practical policy is configuration, not folklore: define a base delay, a maximum delay, a maximum attempt count, and a randomization source; then verify that the resulting schedule fits the queue's actual retention and delivery settings. The source material for Google Cloud Pub/Sub is useful here as an example of a managed publish-subscribe system, but its documented behavior should be read for the exact subscription configuration in use rather than treated as a universal queue contract. Your mileage may vary across brokers, especially around acknowledgement deadlines, retention, ordering, and dead-letter behavior.

The following Go policy returns a retry decision rather than sleeping inside a worker. That is deliberate — an in-process sleep occupies worker capacity, disappears on process termination, and hides the schedule from queue operations.

```go
package retry

import (
	"errors"
	"math/rand/v2"
	"time"
)

var (
	ErrInvalidInput      = errors.New("INVALID_INPUT")
	ErrUnsupportedFormat = errors.New("UNSUPPORTED_FORMAT")
)

type Policy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

type Decision struct {
	Retry bool
	Delay time.Duration
	Code  string
}

func (p Policy) Decide(attempt int, err error) Decision {
	if errors.Is(err, ErrInvalidInput) || errors.Is(err, ErrUnsupportedFormat) {
		return Decision{Code: "PERMANENT"}
	}
	if attempt >= p.MaxAttempts {
		return Decision{Code: "ATTEMPTS_EXHAUSTED"}
	}

	ceiling := p.BaseDelay
	for n := 0; n < attempt && ceiling < p.MaxDelay; n++ {
		if ceiling > p.MaxDelay/2 {
			ceiling = p.MaxDelay
			break
		}
		ceiling *= 2
	}
	if ceiling > p.MaxDelay {
		ceiling = p.MaxDelay
	}

	// Full jitter spreads eligible attempts across the current delay window.
	delay := time.Duration(rand.Int64N(int64(ceiling) + 1))
	return Decision{Retry: true, Delay: delay, Code: "TRANSIENT"}
}
```

The configuration needs validation at startup: all durations must be positive, the maximum must not be below the base, and attempts must have a finite bound. I don't think a fixed default can be defended without workload evidence. An interactive webhook may have a different useful retry horizon from a large render, and an upstream recovery window may be shorter than the queue's retention; tests should inject the random source and assert bounds rather than assert one sampled delay.

Do not retry in a tight loop. It turns dependency pressure into self-amplifying load, and the resulting attempt log is often too compressed to explain which deployment or input class changed. A delayed queue attempt should instead carry the operation ID and attempt ordinal, while the durable ledger retains the classified error code such as `UNSUPPORTED_FORMAT` or `ATTEMPTS_EXHAUSTED`. Those codes are part of the operational contract, not prose scraped from an exception.

## The job ledger makes redelivery safe enough to reason about

The state transition must be conditional. Two workers may receive the same operation, so reading `pending` and later writing `rendered` is a race; the claim must compare the expected version and record who owns the lease. A lease prevents permanent abandonment after a worker exits, but it does not prove that an external effect failed. On lease expiry, the next worker must reconcile each step before repeating it.

Here is a focused worker skeleton. It omits transport-specific methods and leaves persistence behind interfaces, which makes the protocol testable without implying that a particular queue supplies transactional coupling to a database.

```go
package worker

import (
	"context"
	"errors"
)

var ErrLeaseHeld = errors.New("LEASE_HELD")

type Job struct {
	OperationID string
	SourceRef   string
	Variant     string
}

type Record struct {
	OperationID string
	Rendered    bool
	Notified    bool
	Version     int64
}

type Ledger interface {
	Load(context.Context, string) (Record, error)
	Claim(context.Context, string, int64) (Record, error)
	MarkRendered(context.Context, Record, string) (Record, error)
	MarkNotified(context.Context, Record) (Record, error)
	RecordFailure(context.Context, string, string) error
}

type Renderer interface {
	PutVariant(context.Context, string, string, string) (string, error)
}

type Notifier interface {
	Send(context.Context, string, string) error
}

type Worker struct {
	ledger   Ledger
	renderer Renderer
	notifier Notifier
}

func (w Worker) Handle(ctx context.Context, job Job) error {
	record, err := w.ledger.Load(ctx, job.OperationID)
	if err != nil {
		return err
	}
	if record.Rendered && record.Notified {
		return nil
	}

	record, err = w.ledger.Claim(ctx, job.OperationID, record.Version)
	if err != nil {
		return err
	}

	if !record.Rendered {
		outputRef, renderErr := w.renderer.PutVariant(
			ctx, job.OperationID, job.SourceRef, job.Variant,
		)
		if renderErr != nil {
			_ = w.ledger.RecordFailure(ctx, job.OperationID, "RENDER_FAILED")
			return renderErr
		}
		record, err = w.ledger.MarkRendered(ctx, record, outputRef)
		if err != nil {
			return err
		}
	}

	if !record.Notified {
		if err := w.notifier.Send(ctx, job.OperationID, "rendered"); err != nil {
			_ = w.ledger.RecordFailure(ctx, job.OperationID, "NOTIFY_FAILED")
			return err
		}
		_, err = w.ledger.MarkNotified(ctx, record)
		return err
	}
	return nil
}
```

There is a hard limit here: if `Send` applies an irreversible effect but its response is lost, the local ledger cannot infer the remote outcome. Exactly-once delivery is the wrong claim. The defensible target is an exactly-once business effect where the receiving endpoint deduplicates the stable operation ID, or a reconcilable effect where the sender can query before retrying. If neither is possible, duplicates remain part of the interface and downstream accounting must admit them.

Consider one delivery with operation ID `img_01`, rather than an invented promise about queue semantics. The worker claims ledger version seven, writes the requested variant under the deterministic identity formed from `img_01` and the variant name, sends a notification containing `img_01`, and then loses the response before it can mark the notification step complete. On the next delivery, the stored image identity answers the render question: the worker can reconcile that output instead of producing a logically new one. The notification question remains open. If the receiver stores `img_01` under a uniqueness constraint, resending closes the uncertainty without creating a second business event; if the receiver exposes a status lookup, the worker may reconcile before sending; if the receiver supports neither behavior, the ledger must label the outcome ambiguous and an automatic retry would merely exchange uncertainty for a possible duplicate. Notice what the attempt count cannot tell us: attempt two may be the first execution of no step, one step, or both steps. Only per-step evidence resolves that distinction, which is why a single job-level `completed` flag is too weak for redrive approval even though it looks adequate on the success path.

Testing should attack transitions rather than happy-path functions. Deliver the same job concurrently; terminate a worker after the render but before `MarkRendered`; lose the notification response after the receiver records it; expire a lease; and redeliver an old message after completion. Every test should finish with one intended output identity, one logical notification effect, and an attempt history that accounts for every decision. Keep payload fixtures free of sensitive material, because a DLQ and its diagnostics often have broader operational readership than the primary data store.

## A DLQ is quarantine with an admission policy

A dead-letter queue separates jobs that automatic policy will no longer execute from jobs still eligible for delayed retry. It isn't an archive, and redrive isn't a repair. A job placed there needs the operation ID, failure classification, attempt count, relevant state version, and a reference to diagnostic evidence; secrets and full webhook bodies should not be copied merely because they are convenient during debugging.

Before redrive, an operator should identify the failed cohort, confirm that its cause has changed, reconcile already committed steps, estimate the work being reintroduced, choose a rate and batch bound, and record an approval identity. Then replay into the normal input path with the same operation IDs. Generating new IDs would defeat deduplication and sever the audit chain.

| Mechanism | Appropriate use | Principal limitation |
| --- | --- | --- |
| Immediate worker retry | A narrowly bounded local operation | Consumes worker capacity and vanishes with the process |
| Delayed queue retry | A classified transient failure | Repeats any step not protected by durable state |
| DLQ quarantine | Exhausted or permanent failures needing review | Accumulates silently without ownership and alerts |
| Controlled redrive | A reviewed cohort after conditions change | Can reintroduce load and ambiguous side effects in bulk |

The catch is that a queue with convenient automatic redrive is not suitable when the organization cannot inspect, approve, throttle, and stop the replay. In that environment, keep quarantine in a durable store with an explicit re-enqueue tool, even though it adds operational work. Conversely, a database-only scheduler is a poor fit when the team needs the delivery, delay, and consumer-scaling facilities of a queue; retain the queue, but keep business truth in the ledger. No single mechanism owns both concerns cleanly.

Observability should join the two systems without confusing them. Queue metrics describe delivery pressure: eligible depth, oldest age, delivery attempts, and quarantine inflow. Ledger metrics describe business progress: accepted operations, time in each state, classified failures, reconciled duplicates, and notifications awaiting acknowledgement. Alerting solely on queue depth misses a worker that acknowledges jobs before a ledger transition; alerting solely on final completion misses a retry storm. Correlate logs and traces with the stable operation ID, while keeping the append-only audit record separate from mutable telemetry retention.

## What should happen before the retry policy is enabled?

Start in observation mode. Add operation IDs and attempt records, classify failures, and measure the cohorts that the proposed policy would retry or quarantine without changing delivery behavior. Next, make rendering idempotent, separate notification state, and exercise duplicate and crash-boundary tests. Only then enable delayed retries for one transient class with conservative concurrency.

Redrive comes last.

Run it first as a plan that reports eligible, already-completed, ambiguous, and permanently invalid jobs; require a bounded batch and a stop condition; canary a small cohort; reconcile outputs and webhook acknowledgements; then increase the rate while watching both queue pressure and ledger state. Rollback means disabling new replay and allowing in-flight operations to settle, not deleting their audit history. This migration is slower than adding an exponential-delay option to a queue client, but it addresses the actual problem: proving which effects occurred when delivery is repeated.

## Sources

- https://www.rfc-editor.org/rfc/rfc2104
- https://cloud.google.com/pubsub/docs/overview
