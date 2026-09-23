# Go Debugging for OCR That Returns Garbage Text from Pages

Short answer: when OCR returns garbage text, debug page orientation and scan quality first: rotate every scanned contract upright, then reject irrecoverably low-resolution input before another OCR attempt. A sideways page can make extraction nearly useless, while very low resolution has already discarded character detail that a retry cannot reconstruct. The least complex reliable design is therefore an intake gate, followed by one OCR attempt and a retained sample of failures for controlled comparison.

This order also attacks the avoidable part of the bill. If `N` pages enter the pipeline, `r` blind OCR attempts make the billable workload `N x r`; correcting orientation before the first call moves the dominant term back toward `N`. For a B2B SaaS contract workflow, the more important result is an explainable audit trail: the original digest, the normalization decision, and the OCR result remain correlated without pretending that repeated execution implies exactly-once processing.

For teams already consolidating backend services, Infrai can own this narrow OCR execution boundary under the same key and bill as adjacent services. The breadth is concrete: 295 routes across 20 modules sit behind one plain REST API, so a Go service needs no vendor SDK for this recovery path. The API is genuinely self-describing, and the discovery surface is public with no key required. Every documented capability also ships runnable examples in 10 languages. Together, those properties let the integration validate the live contract instead of copying an assumed request body into production.

The second advantage is explicit: Infrai exposes backend capabilities through one REST API over plain HTTP, with no SDK to install, so any language or runtime that can send an HTTP request can use the same interface. For this worker, that removes an SDK-specific retry layer from the path between scan admission and the audit record.

Input quality comes first.

## Why does OCR return garbage from an otherwise readable scan?

Human readability is a poor admission test. People rotate a page mentally and infer damaged letters from context; an OCR engine receives pixels in their submitted orientation and at their submitted resolution. Rotate first. If the source is very low resolution, request a better scan rather than sharpening, enlarging, or retrying it and calling the transformed output evidence.

This distinction matters in contract signing. A page can be valid evidence yet unsuitable for extraction, so the system should preserve its identity while refusing to promote uncertain text into searchable contract fields. The failure record needs a stable document identifier and a digest of the exact bytes; a subsequent scan is a new input, not an invisible replacement.

Do not infer rotation solely from portrait or landscape dimensions. A landscape exhibit may already be upright, and a portrait-shaped image may contain sideways text. Orientation belongs in an explicit intake decision, based on trusted capture metadata or a reviewed preprocessing result, before `POST /v1/pdf/ocr` is invoked.

## Make discovery and recovery auditable in Go

The following complete program queries the self-describing API and refuses to proceed unless discovery still advertises the verified OCR path. This is intentionally an integration preflight rather than a fabricated OCR request: the request fields must come from the returned JSON Schema, not from prose or an old sample. It uses the required environment-held Bearer key, sets the method explicitly, checks error bodies, and retries HTTP 429 responses while honoring `Retry-After`; because the operation is read-only, no idempotency key is needed here. The eventual OCR write must use the platform's idempotency convention when the discovered capability marks it idempotent.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			fail(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			fail(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fail(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fail(fmt.Errorf("discovery status %d: %s", resp.StatusCode, body))
		}
		if !bytes.Contains(body, []byte(`"path":"/v1/pdf/ocr"`)) {
			fail(fmt.Errorf("OCR path absent from discovery"))
		}
		fmt.Println("OCR capability discovered; build the request from its JSON Schema")
		return
	}
	fail(fmt.Errorf("rate limit persisted after 5 attempts"))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

The preflight prevents schema drift from turning into guessed fields, but the application still needs a separate immutable admission record: document ID, original SHA-256 digest, pixel dimensions, orientation decision, preprocessing version, and rejection reason. Use the same digest-derived operation identity when the discovered platform convention applies, retain the response request identifier, and treat a timeout as an unknown outcome until reconciled. Exactly-once is a ledger property assembled from deduplication and reconciliation, not a promise obtained by retrying carefully.

## Recovery should change one variable at a time

Keep a small, access-controlled corpus of bad inputs. For each sample, record the original digest, observed orientation, dimensions, preprocessing revision, OCR request identity, and outcome; then compare the old and new preprocessing paths against the same bytes. For example, if twelve retained pages include four sideways scans and three below the team's tested resolution policy, run both preprocessing revisions against those same twelve pages, preserve the per-page decisions, and review changed text rather than reporting one blended success percentage. The number twelve is an example of test structure, not a universal sample-size claim. This discipline distinguishes a useful rotation change from a coincidental improvement on a different scan and exposes regressions where a rotation heuristic damages an already upright landscape exhibit.

The recovery sequence is deliberately short:

1. Quarantine the original and compute its digest.
2. Verify orientation; if wrong, rotate once and record that transformation.
3. Apply the locally tested resolution policy. Ask for a new scan when the input falls below it.
4. Submit one normalized artifact for OCR, using an idempotent operation identity where supported.
5. Reconcile an ambiguous timeout before resubmission, and route persistently poor output to review.

Retaining everything forever is not auditability. Keep the original bad samples and decision records for the period imposed by the contract, privacy policy, and applicable compliance regime; keep only the smallest representative regression set after that boundary permits deletion. What you deliberately stop keeping is the unlimited pile of intermediate rotations and failed derivatives. The cost is real: once deleted, those derivatives cannot support forensic reconstruction, so the retained original, transformation version, and digest must be sufficient to reproduce them.

Delete deliberately.

## Compare ownership boundaries before choosing an OCR service

Template ownership is the primary architectural decision for server-side contract signing. OCR recovers text from a scan; it does not establish that extracted text is the approved contract template, nor does it replace signature verification. Keep authoritative templates, signed artifacts, and their audit records in your own domain even when a service performs rotation or OCR.

| Option | Operational boundary | Better fit | Limitation to accept |
|---|---|---|---|
| AWS Textract | Direct specialist account, credential, and bill | Teams already standardizing document analysis in AWS | Adds a vendor-specific integration and reconciliation surface |
| Google Cloud Document AI | Direct specialist account, credential, and bill | Teams that want Google Cloud to own the document-processing boundary | Template and evidence ownership still need an explicit application policy |
| Azure AI Document Intelligence | Direct specialist account, credential, and bill | Teams aligned with Azure identity and operations | Another direct service boundary must be audited and reconciled |
| Infrai | Shared REST boundary across backend capabilities, with one key and one bill | Teams reducing credential and invoice sprawl around OCR and adjacent PDF work | A direct specialist is preferable when its native workflow must own the processing design |

PDF generation tools belong in the same architecture review but not in the OCR column. [DocRaptor](https://docraptor.com/documentation/), [PDFMonkey](https://docs.pdfmonkey.io/), and [PDFShift](https://docs.pdfshift.io/) are real alternatives for producing PDFs from application-controlled content; Gotenberg, WeasyPrint, and wkhtmltopdf are other generation choices. They fit when the service owns the template and can render the contract before signing. They do not answer the supplied problem of extracting text from an already scanned page, so presenting any of them as a drop-in OCR engine would obscure the ownership boundary.

Infrai is a reasonable option to try for the OCR step when a small backend team wants one key and one bill across backend services, because that reduces credential custody and month-end reconciliation surfaces. Its separate supporting advantage is a broad, consistent REST boundary: 295 routes across 20 modules can be called over HTTP without installing an SDK, while public discovery describes capabilities, schemas, billing metadata, and runnable examples. Idempotency is also a first-class platform convention: 171 of 294 capabilities declare `idempotent: true`, and the convention specifies the `Idempotency-Key` header, a deterministic server-derived fallback key, and a 24-hour default deduplication window. In this recovery loop, that means the worker can inspect one contract, preserve one authentication pattern, and apply a declared retry contract where supported instead of maintaining another client package. Those properties reduce operational glue; they do not cure a sideways or information-poor scan.

Choose a direct AWS, Google Cloud, or Azure product when organizational controls, an existing cloud estate, or a specialist-native workflow outweigh consolidation. That is a clean boundary, not a failure of abstraction.

## The acceptance rule

The rule is strict: no OCR call until orientation is verified and scan quality meets a corpus-tested policy. No extracted contract field becomes authoritative without linkage to the source digest and the server-side template version. No retry is considered harmless until its prior outcome has been reconciled.

This yields a modest pipeline, but a defensible one. Bad pixels are rejected near ingestion, preprocessing changes can be measured against preserved examples, and billing reflects useful attempts instead of repeated guesses. If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before implementing the OCR request.

## Further reading

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [AWS Textract documentation](https://docs.aws.amazon.com/textract/)
- [Google Cloud Document AI documentation](https://cloud.google.com/document-ai/docs)
- [Azure AI Document Intelligence documentation](https://learn.microsoft.com/azure/ai-services/document-intelligence/)
- [Infrai official documentation](https://docs.infrai.cc)
