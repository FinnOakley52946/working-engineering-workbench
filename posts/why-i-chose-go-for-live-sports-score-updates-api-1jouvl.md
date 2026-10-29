# Why I Chose Go for Live Sports Score Updates API Fan-Out — Reconnects

**Short answer:** For a live sports score updates API serving many viewers from Node.js, use a thin live transport over a versioned event ledger, with replay first and snapshot-plus-tail when the cursor is too old.

I chose a durable event ledger over a clever fan-out cache for logistics typing indicators and read receipts, because a reconnect is a normal delivery state, not an exceptional failure. The live connection can be optimized later; the ordering contract cannot. A driver who briefly loses coverage must not receive a receipt for a message that the client never saw, and a dispatcher must not see a stale "typing" state resurrected by backfill.

This is a deliberate trade: the ledger costs storage, retention rules, and a little more write-path coordination. In exchange, the recovery contract is inspectable. A cache-only design is faster to sketch, but it cannot explain which version a returning viewer missed.

That is the boundary.

No magic.

The same rule applies to live sports score updates: many viewers can receive an ephemeral projection, but every client needs a cursor and a defined recovery boundary. In a Node.js service, I would keep the WebSocket or Server-Sent Events layer thin, assign monotonically increasing stream versions in a durable writer, and make reconnect choose between replay and snapshot-plus-tail. That decision is the API.

## How should a Node.js API deliver live sports score updates?

Typing is transient. A read receipt is a fact about a message. Treating both as identical events is the first design mistake. The second is treating a successful write to the socket as proof that a viewer has consumed the event. TCP gives an ordered byte stream; it does not give the application an acknowledgement that a human-facing client has applied a state transition.

I initially thought the fan-out buffer was the main scaling decision. It was not. The awkward question was what to do with version 900 after a client returned carrying version 742, especially when the retention boundary had moved. In a real recovery review, I would trace the command that created the event, the commit that assigned version 900, the delivery attempt that stopped, and the resume request carrying 742; if any of those records cannot be joined without guessing, the system has a monitoring problem before it has a transport problem.

For a logistics conversation, I would model a message receipt with a message identifier and a client-observed sequence, while modeling typing as a lease with an expiry. The receipt belongs in an auditable record. The typing lease belongs in a replaceable current-state projection, with an event retained only long enough to make reconnect behavior explicit. This distinction also gives a clean answer for a score feed: a score change is durable when it is an authoritative update, whereas a heartbeat or connection presence signal is not.

A minimal event envelope might look like this:

```go
type Event struct {
    StreamID string `json:"stream_id"`
    Version  uint64 `json:"version"`
    Kind     string `json:"kind"`
    Key      string `json:"key"`
    Data     []byte `json:"data"`
}

// ClientAck means the client applied the event, not merely received bytes.
type ClientAck struct {
    StreamID string `json:"stream_id"`
    Version  uint64 `json:"version"`
}
```

The exact serialization is less important than the invariant: a version is scoped to a stream, assigned once, and never reused. An idempotency key belongs to the command that creates a fact; it is not a substitute for the event version. If a mobile client retries a read command, the command handler should return the existing result or a stable duplicate response, while the ledger emits at most one receipt for that command key.

## Can reconnect and backfill be one contract?

Yes, if the service makes the cursor visible and admits that retention has limits. A client reconnects with its last applied version, not with a timestamp rounded to the nearest second. The server checks the requested cursor against the earliest retained version. If the gap is within the replay window, it sends events in ascending order and then resumes live delivery. If the gap is older than retention, it sends a current snapshot carrying a new base version, followed by events after that version.

That is a small state machine, but writing it down prevents several expensive ambiguities. A client must not merge a snapshot from version 900 with events beginning at version 742. It must not advance its cursor when validation fails. It must acknowledge an event after applying it, and it must be safe to receive a duplicate after an acknowledgement is delayed.

```go
type BackfillPlan struct {
    Mode       string
    From       uint64
    SnapshotAt uint64
}

func PlanBackfill(lastApplied, earliest, current uint64) BackfillPlan {
    if lastApplied >= earliest-1 && lastApplied <= current {
        return BackfillPlan{Mode: "replay", From: lastApplied + 1}
    }
    return BackfillPlan{Mode: "snapshot_then_tail", SnapshotAt: current}
}
```

Production code needs an underflow guard when `earliest` is zero, and it needs to distinguish an unknown stream from an empty stream. Those details are not decoration: an invalid cursor can otherwise be interpreted as version zero and silently request a full replay. The server should validate stream ownership before planning recovery, cap the replay batch, and close the connection with a retryable application error if the client cannot keep up.

The phrase exactly once is useful as a design target and dangerous as a transport promise. Across a reconnecting network, I design for at-least-once delivery plus idempotent application, then prove that each business fact has one authoritative record. The audit trail answers what was accepted and when; the client acknowledgement answers what was applied. They are different ledgers.

The limitation is important: snapshot recovery can preserve current truth while skipping intermediate states. That is acceptable for a score projection and usually acceptable for typing, but it is not acceptable for a receipt audit. The event policy has to say which category a stream belongs to.

Choose the boundary before the protocol.

Then test the ugly path.

## How should a fan-out layer behave when viewers multiply?

Fan-out should distribute committed versions, not invent them. One writer appends an authoritative event, and a set of delivery workers can publish that event to interested connections. A slow connection is allowed to fall behind; it is not allowed to hold the writer hostage. Once its queue crosses a bounded limit, disconnecting it and using the documented backfill path is more correct than allocating unbounded memory.

This matters in both workloads. A logistics dispatcher may have a few high-value conversations with strict receipt semantics. A sports score feed may have a large audience watching the same stream, where a current snapshot is often more useful than replaying every intermediate score change. The selection belongs to the event policy, not to an arbitrary connection-count threshold.

I keep three counters separate: committed versions, delivered frames, and applied acknowledgements. A delivery metric can look healthy while clients are failing to apply messages; an acknowledgement metric can look low because users are offline, while the ledger is behaving correctly. Trace identifiers should link the command, committed event, fan-out attempt, and client acknowledgement without putting private message content into logs.

For a Node.js implementation, the same boundaries map naturally to a command handler, a persistence adapter, and a transport adapter. The event loop is good at managing many open connections, but it does not turn an in-memory Map into durable state. The backfill query, ordering check, and idempotency constraint still need a storage design that survives process replacement. A worker restart should replay work from a durable position or rebuild its subscription state; it should not depend on whatever object happened to remain in heap memory.

## Which delivery technology fits the boundary?

A browser-facing WebSocket is a reasonable bidirectional transport for typing signals and acknowledgements. Server-Sent Events can be a simpler one-way choice for score projections when the client only receives updates and the reconnect semantics are explicitly implemented. WebRTC data channels solve a different problem: peer connectivity and media-oriented sessions introduce negotiation and topology choices that do not replace an authoritative event ledger. The transport is downstream of the consistency boundary.

I would compare options by four observable properties: how the client resumes, how the service bounds slow consumers, how authorization is rechecked after reconnect, and how the system exposes delivery versus application. A feature matrix that lists only latency hides the failure mode that hurts operations: two viewers may be connected, but one may be displaying a version from before a correction.

The comparison should also include ownership. A managed connection layer can reduce operational work around connection lifecycle, while an owned layer can make queue limits and replay behavior more directly inspectable. Neither choice supplies business-level exactly-once semantics by itself. In a regulated payment or ledger-adjacent workflow, I would require the provider boundary, retention policy, export path, and audit responsibilities to be explicit before accepting that trade.

For receipts, authorization is part of correctness. A reconnect token should not grant access to a conversation merely because the old socket had access. Recheck membership, stream scope, and token expiry at resume time. For public scores, a viewer may be authorized for a projection while remaining unable to query the source event details. Keeping those data shapes distinct reduces accidental disclosure during snapshot recovery.

## What would I ship first?

I would roll this out in a narrow sequence: define the event envelope and invariants, add a durable cursor to one stream, implement replay and snapshot fallback, then load-test slow consumers and process restarts. Only after those tests pass would I add broader fan-out optimization. The first test is not a latency contest. It is a fault-injection table.

- Append the same command twice with one idempotency key; verify one business fact and one audit record.
- Drop the connection after delivery and before acknowledgement; reconnect from the last applied version; verify a duplicate is harmless.
- Expire the replay window; verify snapshot-plus-tail produces a contiguous client version.
- Delay one consumer until its bounded queue fills; verify the writer continues and the client can recover.
- Revoke stream membership while the client is offline; verify resume is denied before any snapshot is sent.

The rollout should expose `earliest_retained_version`, `current_version`, replay counts, snapshot fallbacks, queue depth, disconnect reasons, and acknowledgement lag. Alerts should describe a broken invariant, such as a gap in committed versions or an acknowledgement arriving for a version the client was not authorized to receive. Raw socket counts are useful capacity signals, but they are not correctness signals.

The decision rule is concrete: use a live transport for immediacy, a versioned ledger for recovery, and a snapshot boundary when replay is no longer bounded. For logistics typing indicators, keep the state expiring and non-authoritative; for read receipts, persist the command result and audit it; for a high-volume score projection, coalesce intermediate views only when the domain permits it. Reconnect is where these promises become visible, so I would design that path before choosing the fan-out mechanism.

## Sources

- W3C, WebRTC 1.0: https://www.w3.org/TR/webrtc/
- IETF, RFC 6455: The WebSocket Protocol: https://www.rfc-editor.org/rfc/rfc6455
- IETF, RFC 9110: HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- MDN, Using server-sent events: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- PostgreSQL documentation, Transaction isolation: https://www.postgresql.org/docs/current/transaction-iso.html
