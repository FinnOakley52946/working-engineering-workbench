# Live Auction Bidding Realtime API Approach in 2026: Fairness and Ordering for Small SaaS

A customer-support editor should treat a cursor as an expiring observation, not as durable business state. The same realtime API approach makes a live auction bidding stream fair: order events per scope, attach a monotonically increasing server sequence, and let clients discard stale observations after reconnect. For a small SaaS team using Node.js, this preserves presence accuracy without pretending that a transient pointer can have the same exactly-once guarantee as a ledger entry.

## Short answer: what should a small team guarantee?

Guarantee ordering within one document and one editor session, not a global order across every connected agent. The server is authoritative for the sequence and the current lease; the client is responsible for rendering the newest valid observation. A disconnect should produce an explicit absence or expiry state, never a cursor that remains visible indefinitely because its last packet happened to arrive first.

Freshness wins.

This distinction matters in customer support. An agent may move a cursor over a sentence while another agent changes the sentence, but the cursor is only a coordination aid. It must not become a hidden write, a permission decision, or an audit record. Those boundaries are where presence accuracy and compliance limits meet.

## Architecture decision record

The invariant is narrow: for a given document, accepted presence events have a server sequence that increases monotonically. Each event also carries an actor identifier, a session identifier, a cursor position, and an expiry deadline. The event is accepted only if the session is authorized to observe that document and its logical timestamp is newer than the last accepted timestamp for that session. A reconnect starts a new session identifier; this prevents an old connection from winning a race after a network partition.

There are three useful boundaries:

| Concern | Authoritative record | Failure boundary |
| --- | --- | --- |
| Cursor visibility | In-memory or short-lived presence state | Expiry, disconnect, or stale sequence |
| Document edits | Durable append or transactional store | Idempotency key and conflict policy |
| Compliance evidence | Append-only audit event | Retention, access control, and export |

The last row deserves discipline. A presence packet may be useful for debugging, but retaining every pointer movement can create unnecessary personal-data exposure. Audit only the security-relevant facts, such as a presence subscription decision or an edit authorization result, under the retention policy that governs the support system. “Exactly once” is a sound mindset for writes; it is a poor promise for ephemeral motion.

## Can realtime ordering keep an auction API fair for a small SaaS?

Arrival order is a transport detail. A client can receive update 42 after update 43 because buffering, reconnect replay, or a proxy changed the path. The server should therefore assign the canonical sequence at acceptance, while the client keeps the largest sequence it has rendered for each session. An update with a lower sequence is dropped silently; a gap can trigger a resync request, but it should not make the client invent an intermediate position. This is also the boundary between an auction's fair bidding order and a merely plausible API display: the display may lag, but the accepted order must be unambiguous.

Here is the critical path in Go. It deliberately keeps authorization and persistence outside the presence reducer: those are separate failure and ownership boundaries.

```go
type Presence struct {
\tSession string
\tActor   string
\tDoc     string
\tOffset  int
\tSeq     uint64
\tExpires time.Time
}

func accept(prev *Presence, next Presence, now time.Time) (Presence, bool) {
\tif next.Expires.Before(now) || next.Offset < 0 {
\t\treturn Presence{}, false
\t}
\tif prev != nil && next.Session == prev.Session && next.Seq <= prev.Seq {
\t\treturn Presence{}, false
\t}
\treturn next, true
}
```

The example is intentionally small. A 30-second lease and a 2-second client refresh window are reasonable starting parameters, not universal truths; tune them against editing cadence and reconnect behavior. Production code still needs a clock policy, bounded offsets, document authorization, serialization around the per-document state, and a resync path. It also needs tests that deliver duplicate, delayed, and replayed messages in different orders. A happy-path test with one browser is not evidence of fairness or presence accuracy.

## Choosing the transport by failure semantics

A persistent bidirectional channel is usually the clearest transport for low-latency cursor events, but its semantics do not remove the need for application-level sequencing. A datagram path can reduce coordination overhead in some deployments, yet it makes loss and ordering explicit; it is a poor fit if the team has not already designed replay and expiry. WebRTC data channels are standardized for peer connections, but peer topology, authorization, and reconnect behavior remain application responsibilities, as the W3C specification makes clear.

For a small SaaS team, the useful comparison is operational rather than brand-based: which channel gives you a bounded reconnect story, observable delivery state, and a simple way to isolate one document's traffic? A Node.js API can expose that contract, but Node.js does not decide fairness; the acceptance sequence and idempotency policy do. Keep the protocol payload boring. A JSON event with a document id, session id, sequence, position, and expiration is easier to inspect than a compact binary format whose debugging costs arrive before scale does.

The rejected option is global total ordering. It sounds fair, but it couples unrelated support documents and turns a local visual hint into a system-wide coordination problem. A global log is justified for a financial ledger, a cross-document workflow requiring a single commit order, or a live auction whose rules explicitly define one shared bidding order. It is not justified for a cursor that should disappear after a lease expires. The limitation is deliberate: scoped order is easier to operate, but it cannot answer a question that requires a cross-scope total order.

## Operations, tests, and the audit boundary

Measure the age of the newest visible cursor, the rate of stale-event drops, resync frequency, reconnect duration, and the number of sessions whose leases expire without a disconnect signal. These metrics answer the actual user question: “Can I trust what I see right now?” They are more useful than a single average latency number. Sample payloads carefully, because cursor coordinates can reveal document content or workflow intent.

Test the reducer with property-based cases for duplicates, reordered delivery, clock skew within the declared tolerance, and two sessions on one document. Then run a browser-level test that kills the connection between sequence 7 and 8, reconnects with a new session, and verifies that the old session cannot reappear. The test is concrete, repeatable, and exposes the stale-connection race that a scripted send-and-receive check misses.

Deploy the presence path separately from durable edit processing when possible. A presence outage should leave editing and audit controls intact, while a document authorization failure should fail closed rather than expose a stale roster. The UI can show “presence unavailable” without claiming that the document itself is unavailable.

The practical conclusion is modest: make cursor state fresh, scoped, ordered, and disposable. Put correctness guarantees where the business meaning lives, and make every weaker guarantee visible in the protocol and the UI.

## References

- W3C, WebRTC 1.0: Real-Time Communication Between Browsers: https://www.w3.org/TR/webrtc/
- RFC 6455, The WebSocket Protocol: https://www.rfc-editor.org/rfc/rfc6455
- RFC 9416, The WebTransport Protocol Framework: https://www.rfc-editor.org/rfc/rfc9416
