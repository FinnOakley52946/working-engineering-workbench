# Express Error Tracking Middleware Setup: 3 Signals for Scheduled Health Imports

Short answer: put the error-tracking middleware at the outer edge of the Express request pipeline, but do not let it be the only detector. A scheduled import can fail without producing an HTTP exception: the job may return an empty result, acknowledge a stale batch, or stop before the request that normally reports completion. The useful design records a request error, a batch outcome, and an auditable cost attribution under one correlation context.

For a healthtech backend, this distinction matters. The question is not merely "did Express throw?" It is "did the scheduled import produce the expected result, and can we explain the operational cost when it did not?"

That is the trap.

## How should an Express middleware setup handle error tracking?

Express middleware sees a request lifecycle. It does not automatically see the lifecycle of a scheduler, an upstream file, or a downstream import transaction. Treat those as three related signals:

1. The request signal identifies the route, tenant or source system, correlation identifier, and terminal HTTP status.
2. The job signal identifies the import run, expected record count, actual record count, start and finish timestamps, and outcome.
3. The accounting signal identifies the cost center or owner responsible for the run, without putting protected health information into an event.

The event should be stable enough to support retries and reconciliation. An event identifier derived from the import run is preferable to a fresh identifier on every retry; otherwise an exactly-once mindset becomes a dashboard illusion. A retry may create several attempts, but it should resolve to one business outcome and one append-only audit trail.

OpenTelemetry describes logs as a signal that can be correlated with traces and other telemetry. That is useful here, but correlation is not proof of completion. Completion belongs to the import state machine: `scheduled`, `started`, `completed`, `empty`, `failed`, or `expired`. The middleware enriches the observation; the job records the fact.

## Why is an HTTP error not enough?

Consider a nightly eligibility import. The scheduler calls `/internal/imports/eligibility`, the handler starts a run, and the upstream responds successfully with zero rows. Express returns `204`. No error middleware runs. Yet zero rows may violate the contract for that source, and the team still needs an alert.

The opposite failure is also common: the handler catches a parser error, writes a partial audit record, and then sends a generic `500`. The error tracker has a stack trace, but the operator cannot tell which run was affected, which source owned it, or whether a retry would duplicate accepted rows. A stack trace is evidence of a software failure; it is not a ledger of business effects. The distinction is operational, and it becomes more consequential when the response and the downstream write do not finish at the same time.

The status code is not the outcome.

The boundary should therefore be explicit. The request middleware creates or accepts a correlation value, captures the terminal result once, and delegates job outcome recording to the import service. It must not mark a run complete merely because `res.end()` occurred. That small rule prevents a successful HTTP response from hiding an empty or stale import.

A compact, language-neutral event shape can look like this:

```go
type ImportObservation struct {
\tRunID           string `json:"run_id"`
\tCorrelationID   string `json:"correlation_id"`
\tSource          string `json:"source"`
\tExpectedRecords int    `json:"expected_records"`
\tActualRecords   int    `json:"actual_records"`
\tOutcome         string `json:"outcome"`
\tCostCenter      string `json:"cost_center"`
\tAttempt         int    `json:"attempt"`
}
```

In an Express implementation, the same fields would be attached to request-scoped context and emitted by one final error handler plus one job-state writer. Keep the event schema separate from the vendor SDK. That makes it possible to test the contract with a fake sink, to change exporters later, and to inspect a replay without reconstructing hidden SDK state.

## How do you attribute cost without distorting reliability?

Cost attribution is a decision axis, not the alert condition. Alert on a missed result, a late result, or an invalid state transition. Attribute the resulting compute, storage, or operator work to the import run after the fact. If cost is placed in the alert threshold, a small but clinically important source can disappear from attention because its volume is low.

Use dimensions with bounded cardinality: source system, environment, import type, and cost center. Do not use patient identifiers, raw filenames, or unbounded error text as metric labels. Put detailed diagnostics in access-controlled logs, with retention and redaction rules that match the data classification. The event can say `parser_error` while a secured record carries the diagnostic detail.

A useful decision record answers four questions: what was expected, what arrived, who owned the run, and what action followed. The last field is important. An alert that is acknowledged without a state transition is not resolution; it is an unclosed audit item.

This is also where idempotency earns its keep. The alert evaluator should consume a durable run outcome, not every retry log. A deterministic key such as `(source, scheduled window, run ID)` lets the evaluator collapse duplicate attempts while preserving attempt count. Reconciliation can then distinguish a retried success from two independent imports.

The limitation is deliberate: bounded dimensions make aggregate cost attribution reliable, but they cannot answer every forensic question. Put high-cardinality diagnostic detail in a secured record and link to it by an authorized identifier. That trade-off protects sensitive data at the cost of a second lookup during investigation.

## A middleware rollout that can be tested

Start with a single import route and a synthetic source that produces three fixtures: a normal batch, an empty batch, and a delayed batch. Assert the HTTP response separately from the import outcome. Then inject a parser failure and verify that the final error handler records one error event, preserves the correlation identifier, and leaves the run in a retryable state.

The test should also exercise the uncomfortable path: the downstream write succeeds, the response is interrupted, and the client retries. The write path needs an idempotency key and a durable result before the retry can be considered safe. Otherwise the observability layer reports a transport error while the business system has already accepted the data.

A minimal Go sink interface illustrates the ownership boundary without coupling the application to an exporter:

```go
type EventSink interface {
\tWrite(ctx context.Context, event ImportObservation) error
}

func RecordOutcome(ctx context.Context, sink EventSink, event ImportObservation) error {
\tif event.RunID == "" || event.CorrelationID == "" {
\t\treturn errors.New("missing import identity")
\t}
\treturn sink.Write(ctx, event)
}
```

The production Express code would adapt its request and error middleware to this interface; the test suite can use an in-memory implementation. Keep the adapter thin. A middleware wrapper that also owns retries, billing, and import state will become impossible to reason about during an incident.

## Choosing the boundary for a real system

The right boundary is the one that preserves causality across a request, a scheduled run, and a reconciled result. A general-purpose error tracker can help collect exceptions, and a logs or traces backend can help correlate them, but neither should be treated as the source of truth for import completion. That source should be the durable job record and its audit trail.

Before rollout, define retention, access control, redaction, and the compliance limit for each field. Health data makes accidental context propagation especially expensive: a correlation ID is useful; a payload copied into every error event is usually not. Record identifiers that let authorized operators retrieve the right secured record instead of duplicating it into telemetry.

Roll out in this order: establish the run state machine, add correlation and error middleware, emit the three bounded signals, then add alerts for missing or late results. Finally, reconcile a sample of runs against the source and destination records. The practical success criterion is not a busier dashboard. It is that an operator can explain one missed import, its retry history, its owner, and its cost without guessing.

## References

- https://opentelemetry.io/docs/concepts/signals/logs/
- https://expressjs.com/en/guide/error-handling.html
- https://www.w3.org/TR/trace-context/
