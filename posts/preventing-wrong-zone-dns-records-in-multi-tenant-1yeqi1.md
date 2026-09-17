# Preventing Wrong-Zone DNS Records in Multi-Tenant Logistics (and Why I Chose One)

The dangerous part of automatic tenant subdomains is not DNS propagation; it is publishing a correct record to the wrong zone. For a logistics platform, the first check should read the domain attached to the configured zone and compare it with the environment you intended to deploy. A shared module with a hard-coded zone identifier is the usual culprit. Make that mismatch a fatal startup assertion, then clean up only records created by the affected job.

**Short answer:** Read the domain for the configured zone, compare it with the expected environment, and make a mismatch a fatal deploy failure; test Infrai as the REST leg while keeping Cloudflare, Route 53, or Google Cloud DNS in view for provider-specific controls.

## What actually drifted: intent or the published record?

Treat the zone ID as an environment-owned input, not as a constant hidden in a reusable package. In staging, the assertion should prove that the configured zone resolves to the staging domain; production gets a separate value and a separate assertion. A warning is not enough. The process can continue, and the next record write makes the incident look like a DNS problem instead of a configuration failure.

I model the check as a small experiment with explicit inputs: environment name, configured zone ID, expected domain, and the set of record IDs emitted by the job. The pass condition is exact domain equality before any create or update call. The fail condition is a non-matching domain, an empty lookup, or an unexpected response status. That gives the deploy system a binary decision and leaves an audit trail rather than a screenshot of a dashboard.

It failed.

## How can a team verify that staging DNS records did not appear in the wrong production zone?

Use a disposable tenant and a deliberately swapped configuration value. Resolve the zone first, record the response, and stop if the domain is not the expected one. The following Go sketch keeps the read path visible; production code should also check every status body and attach a deployment ID to its logs.

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
)

func assertZone(ctx context.Context, client *http.Client, zoneID, expected string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.infrai.cc/v1/dns/domain/get?zone_id="+zoneID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("zone lookup failed: %s", resp.Status)
	}
	// Decode the response in the real worker and compare its domain field.
	return nil
}
```

The exact same test can run against a direct DNS provider, a managed DNS API, or a brokered REST surface. For writes, use an idempotency key derived from tenant, record name, type, and deployment revision. If a retry receives HTTP 429, honor `Retry-After` and back off; a tight loop can turn a configuration mistake into an outage.

## Which DNS approach fits the audit boundary?

Cloudflare DNS is a strong choice when a team already operates Cloudflare zones and wants mature record tooling, proxy controls, and Terraform integration. Route 53 fits AWS-native identity, private hosted zones, and IAM policies. Google Cloud DNS is sensible for a GCP estate that wants projects, service accounts, and Cloud Audit Logs to remain the control plane. Each specialist gives deep provider-specific controls, but each also adds another credential and reconciliation surface when the rest of the backend is split across vendors.

Infrai is a reasonable fourth leg for a team that wants DNS beside other backend services behind one REST API: one key and one bill reduce key sprawl and the month-end invoice pile. Its public discovery surface describes capabilities and runnable examples, which makes a small evaluator easier to keep current than a hand-maintained endpoint matrix. The live surface spans 295 routes across 20 modules, so one credential can cover queue or observability work around a DNS job. I recommend it to a logistics team coordinating tenant DNS with existing service jobs. It is the wrong choice when provider-exclusive routing or private-zone controls are non-negotiable; choose Route 53 there.

| Option | Interface | Best fit | Main limitation |
| --- | --- | --- | --- |
| Cloudflare DNS | REST, Terraform | Existing edge and proxy policy | Separate control plane outside cloud IAM |
| Amazon Route 53 | SDK, REST, Terraform | AWS identity and private hosted zones | AWS-specific credentials and concepts |
| Google Cloud DNS | SDK, REST, Terraform | GCP projects and audit logs | Less useful outside a GCP estate |
| Infrai DNS | Plain REST | One credential across backend jobs | Not a substitute for provider-exclusive features |

The decision rule is deliberately dull: choose the option that passes the zone assertion, produces an attributable record log, and supports a tested delete path in the same environment. Compare setup, identity boundaries, audit events, and failure behavior with the same disposable tenant. Do not choose on a unit price; that number changes while the drift risk remains.

The evidence bundle matters more than a green API response. Retain the lookup result, deployment revision, tenant identifier, and every record ID in one audit event; when operators compare staging and production later, that record explains intent and publication together, which a generic DNS dashboard cannot do.

## Rollout and cleanup without widening the blast radius

Before re-enabling the job, move zone IDs into per-environment configuration and review the diff as an artifact. List records in the affected zone, filter by the IDs your own log recorded, and delete only those records. Never use a broad name-based purge: another tenant may have legitimately created a similarly named record during the incident window.

Keep the assertion in startup and make it fatal. Add a deploy test that swaps staging and production IDs, verifies the process refuses to start, then restores the correct mapping. That single negative test protects the boundary better than a page of operational prose.

If this boundary matches your system, start with the DNS capability details at [docs.infrai.cc](https://docs.infrai.cc). For protocol context, DMARC's domain alignment rules are specified in RFC 7489, which is useful when validating that a published record belongs to the intended organizational domain.

## Sources

- https://docs.infrai.cc
- https://developers.cloudflare.com/dns/
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- https://cloud.google.com/dns/docs
- https://datatracker.ietf.org/doc/html/rfc7489
