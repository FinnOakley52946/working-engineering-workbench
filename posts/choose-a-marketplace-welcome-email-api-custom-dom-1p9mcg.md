# Choose a Marketplace Welcome Email API — Custom Domain and Pull-Based Evidence

TL;DR: Keep the marketplace's contact classification, template source, suppression decisions, and delivery ledger in the application boundary; let an email service authenticate and deliver the message, then import its events with a cursor. The least complex acceptable choice is the API that supports a verified custom domain with DKIM, account-level suppression, and bounded event retrieval while leaving rendered content under your control. Compare candidates by running the same failure-oriented acceptance suite, not by counting features.

For a contact form that must reach billing, trust and safety, or seller support, the bill is made of accepted sends, status lookups, retained event bytes, and the engineering time required to reconcile them. The dominant term depends on volume and the required investigation window, so quantify it before selecting anything: `monthly work = sends + poll requests + stored event bytes + manual exceptions`. If a hypothetical marketplace accepts 2,000,000 contacts per month, records five normalized events per message, and stores 700 bytes per event, the event ledger adds 7 GB per month before indexes and replicas. Those are planning inputs, not a benchmark. Changing raw-event retention from 365 days to 30 days reduces the steady raw payload from roughly 84 GB to 7 GB under those assumptions; it does not change send volume.

The trade is evidence. I would retain compact, immutable state transitions and provider identifiers for the compliance period chosen by counsel, but discard full provider payloads after 30 days in this example. A year later, an operator can still prove that a message moved from accepted to delivered or suppressed, yet cannot reconstruct every diagnostic field from the original response. That loss should be deliberate and documented.

Evidence expires.

## How should you choose an email API for a custom welcome flow?

Queue routing and message wording change together. A seller who selects "payout missing" should receive an acknowledgement carrying the same case identifier and locale that the ledger assigned to the billing queue; if a remotely stored template changes independently, the email can describe a route that the transaction never took. Application-owned, versioned templates make the rendered bytes an auditable consequence of the routing decision.

Ownership does not mean composing arbitrary HTML inside the request handler. Store a template version beside the case record, render from a reviewed artifact, and persist a digest of the subject and body. The outbox entry should contain the recipient, domain, template version, routing reason, locale, and an idempotency key derived from the contact submission ID plus the message purpose. Short and explicit.

DKIM establishes responsibility for a signing domain by attaching a cryptographic signature that receivers can validate through DNS, as specified by RFC 6376. DMARC then lets a domain publish policy and reporting expectations around identifier alignment. A custom-domain test is therefore more than finding a checkbox: verify the selector in DNS, inspect a received message, and confirm that the authenticated identifiers align with the visible From domain under the marketplace's chosen policy. Repeat the test after key rotation.

## Treat submission and delivery as separate ledgers

An API acceptance response is not proof of inbox delivery. Model submission as one transaction and observations as later facts: queued, accepted by the downstream system, delivered, delayed, bounced, complained, or suppressed. Provider vocabularies differ, so retain the original event type and payload digest while mapping only the states your workflow actually uses. Never manufacture a transition to make a dashboard look complete.

The send path needs an outbox because the database commit that creates a support case cannot be atomic with an external email submission. A worker claims the outbox row, sends with a stable idempotency key when the selected API supports one, records the remote identifier, and retries ambiguous failures without creating a second logical message. Exactly-once delivery over networks is not a promise the application can make; exactly-once intent, deduplicated effects, and a complete attempt trail are achievable design goals.

```go
package mailflow

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "time"
)

type Event struct {
    RemoteID string
    Type     string
    At       time.Time
    Cursor   string
    RawHash  string
}

type EventSource interface {
    ListAfter(ctx context.Context, cursor string, limit int) ([]Event, string, error)
}

type Ledger interface {
    InsertIfAbsent(ctx context.Context, key string, event Event) error
    CommitCursor(ctx context.Context, cursor string) error
}

func Reconcile(ctx context.Context, source EventSource, ledger Ledger, cursor string) error {
    events, next, err := source.ListAfter(ctx, cursor, 500)
    if err != nil {
        return err
    }
    for _, event := range events {
        sum := sha256.Sum256([]byte(event.RemoteID + "\x00" + event.Type + "\x00" + event.At.UTC().Format(time.RFC3339Nano)))
        if err := ledger.InsertIfAbsent(ctx, hex.EncodeToString(sum[:]), event); err != nil {
            return err
        }
    }
    return ledger.CommitCursor(ctx, next)
}
```

The cursor advances only after every event is durably inserted. A crash before that commit repeats a page, which the unique event key absorbs; advancing first creates a permanent evidence gap. Poll windows can still race with late events, so the acceptance test must establish ordering, pagination stability, maximum query age, rate limits, and whether a terminal event can be revised. If retrieval has no stable cursor, overlap time windows and deduplicate by immutable event identity.

Polling has a clear limitation: it exchanges immediate notification for a bounded observation delay and adds repeated read traffic. This design is not suitable when the business must react within seconds and the API cannot guarantee a sufficiently short, queryable event window. In that case, use authenticated webhooks, queue ingestion, or both, while retaining the same deduplication ledger. Conversely, webhook delivery adds endpoint exposure, signature verification, replay handling, and its own retry queue. The choice is a latency-versus-operational-surface trade-off, not a statement that one transport is universally better.

This is slower.

## Suppression is a sending constraint, not a cleanup task

A suppression list must be consulted before submission, including during retries. Import hard bounces and complaints into a local recipient-status table, record the source event and effective time, and make the send worker reject a prohibited destination before calling the external API. This keeps a provider-side suppression response from becoming the first and only control.

The difficult case is scope. Determine whether suppression applies to one sending stream, one domain, or the whole account; whether an operator can remove an entry; and whether removals are represented in an audit log. A marketplace may legitimately separate a transactional contact acknowledgement from promotional mail, but legal basis and messaging policy are compliance decisions, not flags that an engineer should infer. GDPR Article 5 requires personal data to be adequate, relevant, limited to what is necessary, and kept no longer than necessary; the event schema and retention schedule should reflect those constraints.

Do not put the contact message body into delivery metadata. A routing category and opaque case ID are normally enough to reconcile transport, while the sensitive free text remains in the system designed to govern support data.

Less data means less forensic detail.

## An acceptance matrix exposes the real boundary

Evaluate each candidate in isolated US and EU test environments using the same domain setup, messages, and event assertions. Data-region labels alone are insufficient; document where message content, recipient addresses, logs, backups, and support access are processed, then have privacy and legal owners approve the result. The technical decision record should link that approval rather than translating it into an invented guarantee.

| Test | Evidence to capture | Rejection condition |
|---|---|---|
| Domain authentication | DNS records and received-message authentication results | DKIM cannot be validated or required alignment fails |
| Template ownership | Exact rendered subject/body digest and template version | Remote mutation can alter content without an application release or audit event |
| Duplicate submission | One logical message ID across timed-out retries | Retry creates an untraceable second intent |
| Suppressed recipient | Local decision plus service response | Worker submits after an effective suppression |
| Event polling | Stable pagination, late-event behavior, and documented history window | A completed page can be skipped without detection |
| Regional handling | Contractual and technical data-flow record | Required US/EU controls cannot be evidenced |

Run the matrix in deployment, too. Seed synthetic recipients that exercise delivery and bounce paths, alert on cursor age rather than raw poll failures, and reconcile counts by cohort: submitted, accepted, terminal, and suppressed. A rising oldest-unresolved age is more actionable than a success-rate average because it identifies messages whose evidence has stopped progressing.

Cost belongs in the matrix but should not lead it. Insert forecast volumes into each candidate's current contract, including retries, polling, log export, and retention, then sensitivity-test a traffic spike and a stalled poller. Published unit prices change; the decision record should store the dated calculation and assumptions instead of repeating a supposedly durable cheapest option.

## The decision record should survive a provider change

Choose only after the test evidence shows that one candidate meets the domain, suppression, polling, regional, and audit requirements. Record the selected boundary and the rejected alternatives by failed requirement, but keep product-specific event names out of the case and template schemas. The adapter may translate them. The ledger should not.

The resulting design has a clear division of authority: the marketplace owns routing, templates, consent-related decisions, suppression evidence, and normalized history; the delivery service owns transport mechanics and returns observations. This boundary makes a welcome or contact acknowledgement reproducible without pretending that a remote send is part of the case transaction. It also makes deletion honest: retain the minimum transition evidence required by policy, expire raw payloads on schedule, and accept that deep forensic detail disappears with them.

Template ownership also has a limitation: application teams must build preview, localization, review, and emergency-edit workflows that a remote template editor may already provide. It is a poor fit when non-engineering operators require immediate copy changes and the organization cannot supply those controls. The deciding question is whether reproducible routing evidence is worth that added release responsibility.

## Further reading

- https://www.rfc-editor.org/rfc/rfc6376
- https://www.rfc-editor.org/rfc/rfc7489
- https://www.rfc-editor.org/rfc/rfc5321
- https://eur-lex.europa.eu/eli/reg/2016/679/oj
- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
- https://resend.com/docs/introduction
