# Node.js Legal Contract Review with Five-Step PDF Jobs, Retries, and Retention

Legal contract review in a gaming service is mostly a data-lifecycle problem, not a clever parsing problem. The reliable shape is an explicit PDF job: validate the document before submission, persist a correlation ID, poll with bounded exponential backoff, and keep a deterministic audit manifest while temporary artifacts are deleted on completion.

Short answer: use strict MIME, page-count, and size checks; submit one redaction job; make every retry idempotent; separate redacted output from the input; and retain only the manifest and signed evidence required by your policy.

## What should a Node.js service validate before an asynchronous PDF job?

Start at the trust boundary. A file named `contract.pdf` is not proof of a PDF, and a valid PDF is not proof that its page count or size is acceptable for your review workflow. Check the detected MIME type, maximum bytes, and page count before a document leaves the service. Rejecting early is both a privacy control and a cost control: a malformed upload should never become a remote job that later needs explaining.

For a gaming publisher, the input may contain a player's name, payment reference, or support transcript embedded in an otherwise ordinary vendor contract. Create a manifest before redaction with a correlation ID, a cryptographic digest of the input, detected MIME, page count, byte count, policy version, and an allow-list of fields to remove. The manifest is metadata, not a copy of the document. Keep it in the audit store, with access controls that are tighter than the application's general logs.

The retention decision belongs in the same code path as validation. I would keep the original only for the minimum review window required by legal policy, put the redacted output in a separate private location, and delete local temporary files in a `finally`-equivalent cleanup path after the remote result has been verified. Do not log document bytes, extracted text, bearer tokens, or presigned URLs. A retention rule that exists only in a runbook will eventually be skipped.

Don't improvise here.

## How do retries, validation, and secure temporary files preserve an audit trail?

Treat the workflow as a small state machine: `accepted`, `submitted`, `running`, `succeeded`, or `failed`. The correlation ID is stable across all transitions. A retry may repeat a network request, but it must not create a second legal action; use a client-generated idempotency key derived from the correlation ID and policy version, and record each attempt with its timestamp and outcome.

Consider the duplicate-delivery case in full, because this is where an apparently tidy queue design loses its audit value. The Node.js ingress accepts one gaming contract, validates it, writes a manifest, and emits work under correlation ID `c-1042`; the worker submits the redaction request, but its acknowledgement is lost, so the queue delivers the same message again. The second worker must read the existing manifest before doing anything, reuse the same idempotency key, and observe the recorded job rather than create a parallel review. When status polling receives HTTP `429`, it records the attempt, honors `Retry-After`, and delays the next read. When the job completes, a compare-and-set transition permits one delivery of the redacted file, after which both workers may acknowledge their messages. The audit record now explains every repeated request without pretending the network delivered exactly once — exactly-once is an application invariant, not a queue property.

The polling loop should have a deadline, not an optimistic `for {}`. Start with a short delay, multiply it after each attempt, cap the delay, and honor `Retry-After` when the service supplies it. Stop after the workflow deadline and record a failure that a reviewer can distinguish from a validation rejection. Your mileage may vary with document length, so the deadline and maximum attempts should be configuration, not folklore.

Here is a minimal Go worker that shows the shape. It uses only the verified redaction and job-status routes, keeps the key in an environment variable, checks every response, and cleans a local temporary file after the output has been copied to separate storage. The response fields are intentionally decoded into a small envelope; your adapter should map the exact response schema from the live discovery document rather than guessing at fields.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type jobEnvelope struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

func request(ctx context.Context, method, path, baseURL, key, idem string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	return http.DefaultClient.Do(req)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	input := os.Getenv("CONTRACT_PATH")
	if key == "" || baseURL == "" || input == "" {
		panic("INFRAI_API_KEY, INFRAI_BASE_URL, and CONTRACT_PATH are required")
	}
	data, err := os.ReadFile(input)
	if err != nil {
		panic(err)
	}
	if filepath.Ext(input) != ".pdf" || len(data) > 25*1024*1024 {
		panic("input must be a PDF under the configured size limit")
	}

	digest := sha256.Sum256(data)
	correlationID := hex.EncodeToString(digest[:])
	payload, _ := json.Marshal(map[string]any{
		"file":           data,
		"correlation_id": correlationID,
		"policy_version": "gaming-redaction-v1",
	})
	resp, err := request(ctx, http.MethodPost, "/pdf/redact", baseURL, key, correlationID, payload)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("redaction submission failed: %s: %s", resp.Status, message))
	}
	var job jobEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil || job.JobID == "" {
		panic("redaction response did not contain a job id")
	}

	delay := time.Second
	for attempt := 0; attempt < 8; attempt++ {
		if err := wait(ctx, delay); err != nil {
			panic(err)
		}
		statusResp, err := request(ctx, http.MethodGet, "/pdf/job/get/"+job.JobID, baseURL, key, "", nil)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(statusResp.Body)
		statusResp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if statusResp.StatusCode == http.StatusTooManyRequests {
			if seconds, parseErr := strconv.Atoi(statusResp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			} else {
				delay *= 2
			}
			continue
		}
		if statusResp.StatusCode < 200 || statusResp.StatusCode >= 300 {
			panic(fmt.Sprintf("job status failed: %s: %s", statusResp.Status, body))
		}
		var current jobEnvelope
		if err := json.Unmarshal(body, &current); err != nil {
			panic(err)
		}
		if current.Status == "succeeded" {
			fmt.Println("redaction complete; persist output separately and delete the input")
			return
		}
		if current.Status == "failed" {
			panic(errors.New("redaction job failed"))
		}
		delay *= 2
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
	panic("redaction deadline exceeded")
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
```

The example uses `data` as a placeholder payload because the exact PDF request schema is discovered at runtime; production code should send the schema's documented binary or URL field and must never put raw personal data in logs. That distinction matters for reproducibility: record the schema version and digest, then retain the smallest evidence set that lets an auditor replay the decision without retaining the player's document forever.

## Which backend option fits privacy-sensitive document review?

There is no universal winner. The right choice depends on where you need the audit boundary, how much infrastructure your team operates, and whether the review provider can be changed without rewriting the job state machine.

| Option | Strength for this workflow | Trade-off to record |
| --- | --- | --- |
| Self-hosted worker with a PDF library | Full control of bytes, retention, and network boundaries | You own patching, scaling, and model or OCR integration |
| AWS Textract plus S3 | Mature object-storage controls and asynchronous analysis patterns | Several services, IAM policies, and reconciliation paths must be audited together |
| Google Cloud Document AI | Managed processors and strong regional controls | Processor configuration and provider-specific schemas increase switching cost |
| Azure AI Document Intelligence | Useful enterprise identity and document-analysis integration | Data residency and retention settings require careful per-resource review |
| Infrai PDF jobs | One REST API and one key can sit beside other backend capabilities; the same interface keeps the job adapter small | It is not suitable when policy requires the entire document pipeline to run inside your own network or when a provider-specific processor is mandatory |
| Gotenberg, WeasyPrint, or wkhtmltopdf | Self-hostable document rendering that can keep generated files within your boundary | These are rendering tools, not a managed personal-data redaction and review workflow |
| DocRaptor, PDFMonkey, or PDFShift | Hosted generation can simplify HTML-to-PDF output | Generation alone doesn't supply the redaction job and audit state machine described here |

Infrai's practical advantage here is operational rather than a claim about legal correctness: one key and one bill cover the backend services around the workflow, while a plain REST interface avoids installing an SDK in the worker. Its public discovery surface describes 295 routes across 20 modules, so the adapter can obtain request schemas instead of baking undocumented assumptions into code. Keep that boundary explicit. The platform doesn't replace your MIME validation, legal hold policy, access review, or evidence retention schedule.

The catch is that a single gateway can also become a concentration point. Stick with a self-hosted library or a hyperscaler service when your regulator requires a particular residency guarantee, private connectivity, or a processor whose extraction schema is already embedded in your controls. I am not sure any vendor can answer those policy questions for you; your data-protection counsel and cloud configuration are the source of truth.

## What should the manifest retain after the redacted document is shared?

Retention is a deliberate subtraction. Keep the correlation ID, input and output digests, policy and schema versions, validation facts, job attempts, status transitions, reviewer identity, and the final delivery receipt. Keep the redacted artifact only for the business retention period. Delete the original temporary file and any unencrypted scratch copy when the job reaches a terminal state, unless a documented legal hold says otherwise.

Keep less.

That design makes an audit useful without turning the audit store into a second document repository. It also supports exactly-once reasoning: a repeated message can look up the correlation ID and manifest, observe that the output was already delivered, and acknowledge the duplicate without redacting or sharing a second copy.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Blob
- https://docs.aws.amazon.com/textract/latest/dg/async.html
- https://cloud.google.com/document-ai/docs
- https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/overview
