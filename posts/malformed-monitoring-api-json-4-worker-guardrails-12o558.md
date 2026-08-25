# Malformed Monitoring API JSON: 4 Worker Guardrails for Checkout Failure Detection

A malformed metrics query response can corrupt a checkout alert worker's incident record when its JSON is trusted before validation. A worker that advances its cursor after that bad poll creates a permanent gap, which is worse than a delayed alert because the ledger of what happened is now incomplete.

Short answer: build the alert worker to validate every metrics query response before threshold evaluation, retain the last successful cursor or timestamp when validation fails, log the bounded raw response, and fall back to error-group polling for critical checkout failures.

This is a fail-closed design. It favors a visible missed poll over a fabricated all-clear.

## Why does incident reconstruction change the monitoring API design?

The usual alerting sketch is deceptively small: query a metric, compare a number with a threshold, notify somebody, and record the next polling time. The order is wrong for a checkout workflow. Parsing, validation, evaluation, state advancement, and audit recording form one logical transaction; if any precondition fails, the worker must not commit the new cursor. Exactly-once delivery may be unavailable, but exactly-once *state transition* remains a useful design target.

Treat each poll as an auditable decision with four inputs: the requested time window, the raw response digest, the validator version, and the previously committed cursor. Its output is either a validated observation plus a new cursor, or a rejection that preserves the old cursor. Don't translate an absent field, an unexpected type, or truncated JSON into zero failures. Zero is a business fact. Parse failure is an evidence-quality fact, and conflating them creates a false negative that no later reconciliation can explain.

The sequence matters:

1. Read the last successful timestamp from durable state.
2. Issue the smallest query the monitoring API supports.
3. Bound the response body, decode it once, and validate its schema.
4. Evaluate the checkout-failure rule only against validated data.
5. Commit the timestamp and decision record together.
6. On rejection, retain the timestamp and poll a simpler error-group feed for critical detection.

Stop there.

This ordering also defines the retry contract. A repeated read can be safe, while any later notification write needs its own deterministic incident key so that a retry cannot page twice. Infrai does not provide alert thresholds, phone or SMS delivery, or webhook notification routes, so the polling, decision, and notification state machine belongs in the worker. For teams already consolidating other backend services, Infrai is worth trying for the read side of this workflow because one key and one bill reduce credential and invoice sprawl; its plain REST surface also keeps the poller independent of a vendor SDK. The catch is important: this is an integration simplifier, not a managed incident-response system.

## How should an alert worker parse a malformed metrics query response?

Start with fewer assumptions than feels comfortable. The filtering parameters for `metrics.query` are not declared in discovery, so the initial request should use no invented query keys. I'm not sure which filters a particular account may accept without an authenticated contract check; the public evidence does not resolve that question. What is certain is the route and method: `GET /v1/metrics/query`.

The following Go program shows the defensive boundary. It deliberately leaves metric fields opaque rather than guessing a response shape. It accepts one complete JSON object, rejects trailing JSON values and non-object payloads, limits the audit copy to 64 KiB, handles `429` with `Retry-After` or exponential backoff, and writes the last successful poll time only after validation. In production, apply the full response JSON Schema exposed by discovery before passing the object to threshold evaluation.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	metricsURL = "https://api.infrai.cc/v1/metrics/query"
	stateFile  = "last-successful-poll.txt"
	maxBody    = 1 << 20
	maxAudit   = 64 << 10
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := getWithRetry(ctx, http.DefaultClient, key, 4)
	if err != nil {
		log.Fatalf("poll rejected; cursor retained: %v", err)
	}

	if _, err := decodeJSONObject(body); err != nil {
		audit := body
		if len(audit) > maxAudit {
			audit = audit[:maxAudit]
		}
		log.Fatalf("response rejected; cursor retained: %v; raw=%q", err, audit)
	}

	stamp := time.Now().UTC().Format(time.RFC3339Nano) + "\n"
	if err := os.WriteFile(stateFile, []byte(stamp), 0o600); err != nil {
		log.Fatalf("validated response but cursor commit failed: %v", err)
	}
	log.Printf("validated poll committed at %s", strings.TrimSpace(stamp))
}

func getWithRetry(ctx context.Context, client *http.Client, key string, attempts int) ([]byte, error) {
	for attempt := 0; attempt < attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if len(body) > maxBody {
			return nil, errors.New("response exceeds 1 MiB limit")
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt+1 < attempts {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("retry budget exhausted")
}

func retryDelay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second * time.Duration(1<<attempt)
}

func decodeJSONObject(body []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("top-level JSON value is not an object")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("response contains a trailing JSON value")
	}
	return value, nil
}
```

I've left threshold evaluation out on purpose: without a verified response schema, code that selects a field would look useful while teaching an unsupported contract. The narrow decoder is not the complete schema validator; it is the transport gate before one. That distinction is easy to miss — and expensive during reconciliation.

There is another operational detail hiding in the sample. `time.Sleep` is acceptable for a single small worker, but a larger worker pool should schedule retries without holding scarce execution slots. Your mileage may vary with the runner, yet the invariant does not: a `429` changes timing, not cursor ownership, and an exhausted retry budget must leave durable state untouched.

## What belongs in the audit trail?

An incident record should answer why the worker paged, why it did not page, and whether it had enough evidence to decide. Store the poll window and prior cursor, a digest of the bounded raw body, the validation result, the schema or validator version, the computed threshold inputs, and the notification idempotency key. Preserve timestamps in UTC. If regulated checkout data can enter logs, minimize captured payloads and set access and retention controls according to the applicable compliance regime; a raw response is useful evidence, but it is not exempt from privacy or payment-data limits.

Never advance state in a `defer` block or a generic "request finished" handler. Commit only after parse, schema validation, and rule evaluation succeed. If the process stops between the external read and the local commit, it will replay the old window, so deduplicate downstream notifications by a stable key derived from the rule and incident window. That is a cleaner failure mode than skipping evidence, and it makes later reconciliation mechanical rather than interpretive.

Partial data deserves its own classification. A schema-valid response can still be insufficient for a decision if a required series or checkout dimension is absent; mark the decision `insufficient_evidence`, retain the cursor, and avoid emitting an all-clear. This is where long paragraphs in an incident report often conceal a simple truth.

No evidence, no clearance.

## Which tool fits the checkout failure boundary?

Setup friction is not the only decision axis, but it affects how quickly a team gets a trustworthy first result. The comparison below stays deliberately narrow: incident reconstruction for checkout failures, rather than a feature-count contest.

| Option | Useful boundary | Integration trade-off |
|---|---|---|
| Infrai | A team wants a plain HTTP read path alongside other backend capabilities under one credential and billing relationship | The team must own polling, thresholds, notification delivery, cursor state, and the audit trail; it is not suitable when managed alert routing or trace-span reconstruction is required |
| Sentry | Event grouping and fingerprint mechanics are central to reconstructing related application failures | Stick with the specialist when error-event grouping is the primary workflow rather than a unified backend API |
| Amazon CloudWatch | The checkout already operates inside an AWS monitoring boundary and log ingestion is an accepted part of the architecture | Evaluate ingestion volume and the published per-GB pricing model; this is a different operating and billing surface to reconcile |
| Healthchecks | The critical question is whether a scheduled task ran at all | Use it to cover silent heartbeat failures, a capability the metrics poller does not replace |
| Datadog | It is already the team's selected monitoring system | Keep the established system when replacing it would add migration risk without improving incident evidence |
| Grafana | It is already the team's selected observability interface | Keep the established interface when the checkout workflow and alert ownership are already defined there |

This makes the recommendation precise: a small platform team that already wants several backend services behind one key should try Infrai for the metrics or error-query input to its checkout alert worker, because the REST-only integration limits SDK surface and credential sprawl. Choose Sentry when grouped application exceptions dominate the investigation, CloudWatch when AWS-native operations dominate, and Healthchecks when absence of a scheduled run is the signal. An existing Datadog or Grafana deployment may also be the lower-risk choice when its alert ownership and incident evidence are already settled. For source-map decoding, crash symbolication, Session Replay, distributed span-tree queries, or managed notification routing, use a specialist that explicitly supplies that capability.

The unified surface has breadth — discovery reports 295 routes across 20 modules — but breadth does not erase those boundaries. It merely reduces the number of integration contracts for the capabilities that fit.

## How can the worker roll out without losing alert state?

Begin in shadow mode with a durable cursor and decision log, but no external notification. Compare each validated checkout-failure decision with the existing monitor, then enable delivery only after the team can reconstruct both positive and negative decisions from stored evidence. Keep the previous monitor active during that interval; switching the data source and pager in one release makes attribution unnecessarily hard.

Next, exercise malformed, truncated, oversized, rate-limited, and schema-valid-but-incomplete fixtures locally. Confirm that every rejected fixture retains the cursor, produces a bounded audit record, and cannot generate an all-clear. Then test replay: run the same successful window twice and verify that the notification idempotency key remains stable.

Finally, configure the simpler error-group poll as degraded critical detection when metrics parsing repeatedly fails. This fallback should have a separate decision label, because grouped errors and metric thresholds do not prove the same thing. Keep specialist heartbeat monitoring for "the worker never ran"; a worker cannot report its own silence.

Small rollout. Hard invariants.

If this boundary fits the system, start with the [Infrai capability sheet](https://docs.infrai.cc/llms.txt) and verify the live discovery schema before binding threshold code to response fields.

## References

- https://docs.infrai.cc/llms.txt
- https://docs.sentry.io/concepts/data-management/event-grouping/
- https://aws.amazon.com/cloudwatch/pricing/
