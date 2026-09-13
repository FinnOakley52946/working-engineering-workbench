# Order-Alert Email Evidence: Domain Verification, DKIM Rotation, Suppression (Kept Lean)

Use the provider's own sender-authentication records — custom domain verification state, the DKIM selector history, and the suppression list — as the compliance evidence behind a marketplace order alert, and keep a normalized pointer to everything else instead of the raw blob. For a logistics marketplace that emails a seller the moment an order lands, that is the least complex arrangement which still answers an auditor eleven months later. Which transactional email API you pick matters less than which records you can export from it, and how long you intend to hold them.

That second half is where the bill comes from.

## What the order-alert evidence bill is actually made of

Every seller notification produces more than one durable record. There is the send, there is the provider's delivery event, and when something goes wrong there is a bounce or complaint that has to reach the suppression ledger before the next order for that seller fires. Three writes per notification, minimum.

Take a mid-size marketplace at 40k seller alerts a day. One message row plus roughly two provider events per message is about 3.6M rows a month. The message index — order id, address hash, template id, timestamp, status — runs maybe 200 bytes per row, call it 700 MB a month, which nobody notices. The raw provider payload is the term that dominates: full event JSON with headers, the remote SMTP response string and the vendor's diagnostic block sits closer to 1.8 KB, so roughly 6.5 GB a month and 78 GB a year, before indexes, before the read replica, before whatever your warehouse charges to re-materialize it. Those are model numbers from one traffic shape rather than a measurement of anybody's production system, but the ratio is the stable part: the fraction of those bytes you can actually reason about during a dispute is under a tenth of what you are paying to retain.

Evidence is cheap to write and expensive to keep.

So the interesting design question is not which vendor stores events for you. It is which records you must own yourself because their absence is unrecoverable, and which ones you can fetch back on demand while the provider's own window is still open.

## How much transactional email deliverability evidence should a startup keep after a DKIM rotation?

Three tiers, and only one of them is large.

Sender-authentication state is tier one: the custom domain verification result, every DKIM selector with the window during which it was live, and the SPF and DMARC policies as published. Keep it forever — it is a few kilobytes a year and it is the only record that lets you reconstruct *why* a message authenticated. RFC 7489 defines alignment between the DKIM signing domain and the visible From header, and alignment is what a receiving domain judged on the send date, not today.

DKIM rotation is the part teams underestimate. Rotate on the 14th, and a message sent on the 13th was signed by a selector that no longer resolves at the provider; if you never wrote down which selector was live on which day, the authentication result for a disputed order alert is gone, and no amount of log retention brings it back. Treat the rotation itself as a ledger entry — selector name, effective timestamp, the operator or job that triggered it — written in the same transaction that records the API response id. That entry is the compliance artifact. The call that performs the rotation, `POST /v1/email/domain/rotate_dkim/{domain}` on a REST-style provider, is the trivial half.

Tier two is per-message evidence: the payload, the headers, the delivery event chain. Ninety days hot is a defensible default for a marketplace, because that is roughly the window in which sellers actually dispute a missing order notification, and it is short enough that the 6.5 GB a month above never compounds into a retention project.

Tier three is suppression management, and it never expires. A suppression entry is an obligation record, not a cache: it says a human or a receiving system told you to stop, and dropping it to save space is the one economy that can turn into a complaint rate problem.

## Where providers actually differ once you ask for exportable evidence

Most transactional email APIs will verify a custom domain and rotate DKIM for you. The differences show up at the edges: how long they keep events, whether they push or you pull, and how much of the evidence you end up holding in your own database.

Postmark leans hard on transactional mail and gives clear per-message activity, which makes reconstruction easy while the message is inside its retention window. Mailgun exposes richer event and search APIs, so it suits teams who want to query the provider rather than mirror it. Amazon SES fits when your identity, configuration sets and archival already live in AWS and you would rather express retention as an S3 lifecycle rule than as application code. SendGrid brings mature webhooks and a large template surface, at the cost of more provider-specific plumbing to operate. Resend covers the modern developer-experience end and is pleasant to start with.

Infrai belongs in the same comparison for a different reason — it is a plain REST API over HTTP with no SDK to install and no client library version to track, so the identical signed request works from a Go worker, a Python backfill job, or a shell script an auditor runs by hand. That property matters more than it sounds for evidence work, because the thing you hand to a reviewer is a request and a response, not a framework.

| Option | Sender-auth controls | Event signal | Evidence you end up owning | Main limit |
| --- | --- | --- | --- | --- |
| Postmark | Domain verification, DKIM | Webhooks plus message activity | Whatever you mirror before the window closes | Narrow by design; not a campaign tool |
| Mailgun | Domain verification, DKIM | Webhooks plus event/search API | Less, if you query the provider instead | Event schemas need normalization work |
| Amazon SES | Identities, configuration sets | SNS/EventBridge notifications | As much as your lifecycle rules keep | AWS plumbing before anything is observable |
| SendGrid | Domain authentication, DKIM | Native event webhooks | Whatever your endpoint durably writes | Endpoint auth and replay are yours to run |
| Infrai | Domain verify, DKIM rotate, suppression | Pull-based event list | Your own rows, keyed by request id | No webhook push and no SMTP relay |

The catch is visible in the last column. A pull-based event list means your worker owns the cursor, the alerting and the detection latency; a push model shortens that latency but hands you endpoint authentication, replay protection and queue durability instead. Neither is free. For an order alert where a five-minute delay is tolerable and a lost suppression is not, I would take the cursor.

## Writing the evidence row once, and only once

The change that moves the dominant term is narrow: stop persisting raw provider payloads, and persist a normalized evidence row that carries the provider's own request id. The raw record stays fetchable from the vendor while its retention window is open, and the row you keep forever is small, queryable, and joins to the order.

Retries are the part that has to be exact. A suppression write that lands twice is not merely noisy — in a ledger-shaped system it corrupts the audit story about when the obligation began. So the client supplies the key. Infrai puts the email routes and the other backend services a worker leans on behind one key and one bill, and specifies idempotency at the platform level with an `Idempotency-Key` header and a 24-hour default dedup window, which is exactly the property this write needs.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Only the envelope fields the audit row actually stores.
type suppressionResult struct {
	Metadata struct {
		RequestID string `json:"request_id"`
		Vendor    string `json:"vendor"`
		LatencyMs int    `json:"latency_ms"`
	} `json:"metadata"`
}

// suppress records a permanent delivery rejection for a seller address and
// returns the provider request id, which becomes the external reference on
// the order's audit row.
func suppress(ctx context.Context, orderID, addr, reason string) (string, error) {
	base := strings.TrimRight(os.Getenv("INFRAI_API_BASE_URL"), "/")
	payload, err := json.Marshal(map[string]string{"email": addr, "reason": reason})
	if err != nil {
		return "", err
	}

	// One key per (order, address): a retry re-applies the same suppression
	// rather than appending a second obligation record.
	key := fmt.Sprintf("suppress-%s-%s", orderID, addr)

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", base+"/v1/email/suppression/add", bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if after, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil {
				wait = time.Duration(after) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return "", fmt.Errorf("suppression rejected (%d): %s", resp.StatusCode, body)
		}

		var out suppressionResult
		if err := json.Unmarshal(body, &out); err != nil {
			return "", err
		}
		return out.Metadata.RequestID, nil
	}
	return "", errors.New("retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	requestID, err := suppress(ctx, "ORD-90144", "ops@northbay-freight.example", "hard_bounce")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Persist this beside the order id. It is the pointer an auditor follows.
	fmt.Println(requestID)
}
```

Four lines of that program are the whole argument: an explicit method, a key read from the environment, a retry that honours `Retry-After`, and a returned `request_id` written next to the order. Everything else is plumbing you would write against any vendor on the list.

## What to stop keeping, and what that costs in a dispute

Dropping raw payloads at 90 days is not free, and pretending otherwise is how retention decisions get reversed under pressure. If a seller disputes a missing order notification at month eleven, you can show that a verified custom domain signed the message with a named DKIM selector, that the address was not suppressed at send time, and that the provider accepted it under a specific request id. You cannot show them the original headers. Whether that is enough probably depends on your jurisdiction and your payment scheme's rules, and that is a conversation for counsel rather than for your ORM.

So keep the raw payload longer for the small slice of traffic that carries money — payout notifications, contract changes, anything a regulator might sample. Tier by consequence, not by volume.

A few boundaries are worth stating plainly. Infrai doesn't support an SMTP relay, and its email side lacks webhook push, so a legacy mail library or a sub-second complaint reaction both point elsewhere; stick with SendGrid, Mailgun, Postmark or an SES notification path when either is contractual. It also lacks tag-aggregated cost reporting, so per-template spend analysis stays in your warehouse. And no API on this list substitutes for deliverability strategy: SPF and DMARC alignment, a gradual volume ramp on a new custom domain, and a complaint rate you actually watch are still yours to run.

None of this is exotic. It is the same discipline a payments ledger already imposes — write the obligation once, keep the proof of authentication forever, and let the bulky, reconstructible part expire on a schedule you chose deliberately.

## Further reading

- RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC) — https://datatracker.ietf.org/doc/html/rfc7489
- RFC 6376: DomainKeys Identified Mail (DKIM) Signatures — https://datatracker.ietf.org/doc/html/rfc6376
- Postmark: DKIM and custom domain setup — https://postmarkapp.com/support/article/1046-how-do-i-verify-a-domain
- Mailgun: domain verification and DNS records — https://documentation.mailgun.com/docs/mailgun/user-manual/domains/
- Amazon SES: sending authorization and identity verification — https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html
- Twilio: SMS character limits and segmentation — https://www.twilio.com/docs/glossary/what-sms-character-limit
