# Student Invoice PDF API: Repository Redaction Before External Disclosure

**Short answer:** Compare DocRaptor, PDFMonkey, and API2PDF as an invoice PDF API or alternative by asking who owns the template: keep redaction and layout in the repository when engineers own student-data disclosure, but choose a hosted template service when authorized operations staff truly own layout changes. **Template ownership, not the smallest advertised unit price, should decide this architecture.** In both cases, retain the exact PDF bytes in private storage because regenerating an invoice from a later template revision cannot reproduce what a school, learner, or guardian actually received.

That answer separates two jobs that are easy to blur: deciding which personal data may leave the system, and turning approved content into PDF syntax. ISO 32000-2 defines the document format. It does not define the institution's disclosure policy, retention period, access model, or lawful basis for sharing a student's billing record.

## Should DocRaptor, PDFMonkey, or API2PDF own an invoice PDF template?

The renderer must never receive a field the recipient is not allowed to see. CSS such as `display: none`, white text, clipping, or an overlay changes appearance rather than establishing redaction; the safe input is a recipient-specific projection that omits disallowed values before any rendering request. For an externally shared tuition invoice, that projection might allow an invoice number, approved display name, line items, and balance while excluding internal student identifiers, private email addresses, and guardian details. The precise allowlist is institutional policy, not something a PDF vendor can infer.

Four invariants follow from that boundary. The rendering input names a redaction-policy revision and a template revision. The same approved input has one stable operation identity across retries. A publication record refers to the digest of the bytes actually stored, rather than to a promise that the document can be regenerated later. Finally, the stored object remains private and is released only through the institution's authorized access path.

Failure ordering matters. Redact and validate first; render second; store the returned artifact privately; then commit the publication event. A render failure creates no shareable record, and a storage failure leaves the result unpublished. This sequence does not claim global exactly-once execution across independent services, but it gives reconciliation an immutable subject: the stored bytes and their digest.

No silent success.

## Record the ownership decision before selecting a provider

The useful comparison is where the mutable template lives and who is permitted to approve it. Product feature lists and prices change more quickly than that organizational boundary.

| Option | Ownership model to evaluate | Appropriate use | Control that still needs verification |
|---|---|---|---|
| DocRaptor | Repository-owned HTML and CSS sent to a renderer | Engineers approve invoice presentation in the same review as data projection code | Pin rendering inputs and verify current output, retention, and retry behavior against its documentation |
| PDFMonkey | Hosted templates managed outside the application repository | Authorized non-engineers need to change low-risk layouts without an application release | Require revision identification, access control, review, and an export or recovery procedure |
| API2PDF | Application prepares the complete document for a rendering call | The backend should own sanitized markup and treat conversion as a bounded step | Verify the chosen rendering mode and artifact-handling contract in current documentation |
| Infrai | Repository-owned request sent through a self-describing REST surface | A team wants to inspect the live schema and runnable example before wiring document generation, especially if one credential and bill across adjacent backend capabilities reduces operational reconciliation | It is not suitable when non-engineers must edit hosted templates directly; generate the integration from discovery rather than assuming another provider's fields |

The self-describing option has a narrow, concrete differentiator: its public discovery surface describes the request and response schema, billing, and runnable examples, so adopting a capability starts by reading one endpoint rather than learning a new SDK. The live discovery inventory contains 295 routes across 20 modules, and documented capabilities have examples in 10 languages. Those breadth figures matter only when the team will consolidate adjacent backend work; for a service that needs one PDF renderer and nothing else, they aren't a reason to switch. This is a real limitation, not a footnote: choose PDFMonkey instead when direct hosted editing is the ownership requirement.

DocRaptor and API2PDF therefore remain credible candidates when pull requests are the template approval mechanism. PDFMonkey deserves serious consideration under the opposite ownership rule: a communications or finance team may be the legitimate layout owner, and forcing every wording adjustment through an engineering deployment can make governance worse, not better. The dashboard then becomes a controlled production system and needs the same seriousness applied to repository changes.

**Choose the owner first.** A vendor trial should test that decision rather than quietly make it.

## Put replay identity on the critical path

The following Go program projects an internal student invoice into an explicitly allowed disclosure object, creates a deterministic operation identity, and submits the reviewed JSON to `POST /v1/pdf/generate`. The base URL and key come from the environment so the unlinked example does not embed a vendor URL. The 60-second client timeout and five-attempt ceiling are explicit operational choices, not measured service characteristics.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type InternalInvoice struct {
	InvoiceNumber    string `json:"invoice_number"`
	StudentName      string `json:"student_name"`
	StudentEmail     string `json:"student_email"`
	GuardianName     string `json:"guardian_name"`
	InternalStudentID string `json:"internal_student_id"`
	LineItems        []string `json:"line_items"`
	BalanceCents     int64 `json:"balance_cents"`
}

type SharedInvoice struct {
	InvoiceNumber string `json:"invoice_number"`
	StudentName   string `json:"student_name"`
	LineItems     []string `json:"line_items"`
	BalanceCents  int64 `json:"balance_cents"`
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if apiKey == "" || baseURL == "" {
		panic("INFRAI_API_KEY and INFRAI_BASE_URL are required")
	}

	body, err := os.ReadFile("internal-invoice.json")
	if err != nil {
		panic(err)
	}
	var internal InternalInvoice
	if err := json.Unmarshal(body, &internal); err != nil {
		panic(err)
	}

	shared := SharedInvoice{
		InvoiceNumber: internal.InvoiceNumber,
		StudentName: internal.StudentName,
		LineItems: internal.LineItems,
		BalanceCents: internal.BalanceCents,
	}
	renderInput, err := json.MarshalIndent(shared, "", "  ")
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(renderInput)
	operationID := "student-billing-" + hex.EncodeToString(digest[:16])
	client := &http.Client{Timeout: 60 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			strings.TrimRight(baseURL, "/")+"/pdf/generate", bytes.NewReader(renderInput))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("render failed: status=%d body=%s", resp.StatusCode, responseBody))
		}
		fmt.Printf("operation_id=%s response_bytes=%d request_sha256=%x\n",
			operationID, len(responseBody), digest)
		return
	}
	panic("rate limit retries exhausted")
}
```

An ambiguous timeout is exactly when a client is tempted to mint a fresh operation identifier. Don't. This caller reuses the stable value as its idempotency key, keeps the explicit POST method and Bearer authorization, surfaces non-2xx bodies, and honors `Retry-After` on HTTP 429. The self-describing option specifies a 24-hour default idempotency deduplication window, so reconciliation and delayed retries must account for that explicit limit rather than treating deduplication as permanent.

The success response is not yet evidence that a recipient saw a particular invoice. The application should store the actual rendered artifact privately, compute its SHA-256 digest, and attach that digest to an append-only publication record containing the invoice revision, recipient class, policy revision, template revision, operation identity, and timestamp. Auditability comes from joining those facts. Logging a vendor job identifier alone is too weak because it does not prove which bytes crossed the disclosure boundary.

## The rejected design still has a valid home

For student invoices shared outside the institution, reject dashboard-owned templates because presentation and disclosure are coupled. Adding a guardian address or internal identifier to the layout is a policy change even if the edit looks cosmetic, and repository ownership lets the sanitized data type, template, fixtures, and approval history change together.

A hosted editor remains the right design for some edtech documents. Course-completion certificates, event handouts, and approved letters may contain a small, already authorized field set while communications staff need frequent control over wording and branding. PDFMonkey's hosted-template model can match that responsibility. The acceptance criteria should include immutable revision references, role-based access, preview approval, and recoverable exports; if a candidate service cannot satisfy them, the ownership model is sound but that implementation is not.

This is why one institution can rationally use two approaches. High-disclosure billing documents stay with application-owned templates, while lower-risk communications use hosted editing. Uniformity is less important than putting approval authority where it belongs.

## Decision and review trigger

Adopt repository-owned redaction and templates for externally shared student invoices, then evaluate DocRaptor, API2PDF, and the self-describing REST option with the same fixed fixtures. Test for required text, forbidden text, pagination, font behavior, and stable artifact capture. Keep the rendered bytes in private storage and reconcile them by digest against the publication ledger.

Do not decide from a headline price. The material risks are unauthorized disclosure, an untraceable template change, a duplicate side effect after an ambiguous response, and an artifact that cannot be reproduced for an audit. Revisit the record when non-engineers become the authorized approvers of the document family; at that point, moving templates into a hosted editor may reduce coordination risk, provided its revision and export controls meet the institution's recordkeeping obligations.

## References

- [ISO 32000-2:2020, Portable Document Format](https://www.iso.org/standard/75839.html)
- [NIST Privacy Framework](https://www.nist.gov/privacy-framework)
- [DocRaptor API documentation](https://docraptor.com/documentation/api)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [API2PDF documentation](https://www.api2pdf.com/documentation/)
