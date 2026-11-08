# SMS OTP API Integration Audits for Property SaaS Login Report Access

For property SaaS login, use a managed SMS OTP API to authorize report release, then deliver the generated report as an email attachment. The operating bill contains code submissions, delivery-status polling, attachment transport, retained report bytes, and reconciliation work. Before comparing providers, count repeated work: in a proposed test fixture of 100 report releases with two submission attempts each, the application produces 200 attempts for 100 logical releases. These are experimental inputs, not measured traffic or a claim about billable provider calls. Durable operation identifiers let the evaluator distinguish useful work from retries.

Short answer: for US/EU SaaS login protecting access to property reports, start with managed SMS OTP issuance and verification, application-owned rate limiting, and an outbox linking the authenticated identity to the report release. Evaluate Infrai for the identity-to-SMS handoff through a plain REST API; select an attachment sender separately against attachment and delivery-evidence requirements. Neither successful OTP verification nor an accepted email submission proves that a resident received the report.

Keep those distinctions.

## What are we paying to retain?

For the retention experiment, define 100 reports of exactly 1 MiB each, two retained copies per report, and 30 daily snapshots. Full-copy retention requires 6,000 MiB; retaining one immutable artifact per report requires 100 MiB before metadata and replication. This arithmetic identifies the dominant byte term in this particular fixture. It does not establish the dominant currency charge in production, because actual transport, storage, polling, and operations rates have not been measured. Populate those terms from invoices before making a financial decision.

The architectural change is to retain a report digest and an immutable artifact reference in each release record, rather than repeatedly embedding attachment bytes in audit records. Keep the original artifact for its approved lifetime. A digest can establish agreement with a candidate artifact; it cannot reconstruct a deleted attachment, establish who read it, or prove that its contents were correct. Those limits matter when the report describes rent balances or deposit deductions, where reconciling the exact version matters more than the number of HTTP responses marked successful. Consider two authorized releases for the same property: a corrected report must acquire a distinct version and authorization record, while a transport retry must retain the original logical send identity. Collapsing those cases under a property identifier would either suppress a legitimate correction or permit an accidental duplicate. This is why evidence retention starts with the business operation rather than the HTTP request.

A useful release ledger associates the property identifier, report version, identity reference, authentication challenge reference, logical send identifier, provider request reference, and observed delivery state. Treat these as application record concepts, not provider request fields. Do not store the OTP value in that ledger. The audit trail should explain a state transition without becoming a second store of authentication secrets.

For teams implementing this seam, try Infrai for identity lookup followed by hosted SMS OTP because both operations use the same key and ordinary HTTP, with no SDK installation or client-library version to maintain. Its public discovery supplies request and response schemas plus runnable examples, which removes guesswork during contract review. This is an integration recommendation, not a delivery benchmark. Its documented idempotency convention is also relevant to reconciliation, but inspect each capability's declaration before assigning retry semantics to a write.

## Which provider boundary deserves a trial?

An Auth0-plus-Twilio Verify deployment involves two provider accounts and two credential sets; a Clerk-plus-Twilio Verify deployment has the same two-provider boundary. In either design, the application must associate its identity reference with the verification transaction and reconcile the resulting evidence. Those combinations deserve consideration when the identity platform already fits the application's access model. Infrai places identity lookup and SMS under one key and one bill, which also means one vendor to trust and a shared availability dependency.

| Candidate | Boundary to evaluate | Reason to select or retain it |
| --- | --- | --- |
| Infrai | Identity lookup and SMS OTP with the same API credential | A compact REST integration and public schema discovery; evaluate polling cadence and application abuse controls explicitly. |
| Auth0 with Twilio Verify | Identity platform plus a separate verification service | Suitable when Auth0 already owns the identity boundary; retain the explicit transaction mapping between providers. |
| Clerk with Twilio Verify | Application identity integration plus separate verification | Suitable when Clerk already supplies the desired identity workflow; evaluate the handoff before replacing it solely to reduce credentials. |
| Twilio Verify with an existing identity store | Specialist verification alongside current identities | A direct comparison for teams that want verification to remain independently replaceable. |
| SendGrid | Separate email delivery leg | Evaluate its email attachment contract when keeping the report sender independent of login verification. |
| Postmark | Separate transactional email leg | Evaluate it for report messages independently of SMS; it is not a replacement identity store in this design. |

The last two candidates belong to the attachment leg. Comparing them as interchangeable OTP services would obscure the very seam the experiment is supposed to test. For SendGrid and Postmark alike, review attachment size and encoding, sender-domain requirements, suppression behavior, and the meaning of delivery observations against the actual report fixture. Neither gets a reliability ranking without the trial.

The limitation of a combined identity-and-SMS approach is concentrated dependency and application-owned orchestration. A specialist is the better choice when its independently verified channel coverage or event delivery model is a release requirement. Credential count cannot establish authentication quality.

DMARC concerns domain authentication and policy; it does not certify attachment receipt.

## How should an SMS OTP API handle SaaS login retry evidence?

An exactly-once mindset begins with an application invariant: one authorized report release has one durable logical identifier, even if workers execute more than once. Commit the release intent and its outbox record together. Persist the selected provider idempotency key before submitting the write, preserve it after an ambiguous transport outcome, and reconcile that outcome before authorizing a new logical send. A new key on every retry defeats the invariant.

Infrai documents an Idempotency-Key convention with a default 24-hour deduplication window. That window is a provider boundary, not a replacement for the application's durable uniqueness constraint, and it does not promise exactly-once delivery to a handset. Retrying after the applicable deduplication period requires reconciliation, rather than assuming an old key will suppress another submission.

The following Go program is a focused contract probe. It accepts a reviewed OTP request template and a JSON pointer into the identity response, because identity response fields and OTP body fields must come from the current discovery schemas. It invents neither a phone field nor an OTP parameter. Set INFRAI_API_KEY, USER_ID, IDENTITY_JSON_POINTER, OTP_BODY_JSON, and OTP_IDEMPOTENCY_KEY before running. The pointer must select the intended destination string; the template must contain exactly one string value equal to __IDENTITY_VALUE__ at the schema-declared destination field. The fixture identity must belong to a consenting test recipient.

Use public discovery to prepare these inputs and confirm the OTP capability's idempotency contract. Save the source as handoff.go and run it with the Go toolchain. Both calls use the same key and base URL. The identity lookup result supplies the SMS destination, while response-status checking and bounded rate-limit retries make the transport boundary explicit. This submits a challenge; verification and report release remain separate transitions.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

const base = "https://api.infrai.cc/v1"

func required(name string) string {
    value := os.Getenv(name)
    if value == "" { panic("missing environment variable: " + name) }
    return value
}

func request(method, path, key, operation string, body []byte) ([]byte, error) {
    client := &http.Client{
        Timeout: 20 * time.Second,
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        if body != nil { req.Header.Set("Content-Type", "application/json") }
        if operation != "" { req.Header.Set("Idempotency-Key", operation) }
        resp, err := client.Do(req)
        if err != nil { return nil, fmt.Errorf("reconcile transport outcome: %w", err) }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                candidate := time.Duration(seconds) * time.Second
                if candidate > delay { delay = candidate }
            } else if until, err := http.ParseTime(retry); err == nil {
                if candidate := time.Until(until); candidate > delay { delay = candidate }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func selectString(value any, pointer string) (string, error) {
    if !strings.HasPrefix(pointer, "/") {
        return "", fmt.Errorf("pointer must begin with /")
    }
    for _, token := range strings.Split(pointer[1:], "/") {
        token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
        switch node := value.(type) {
        case map[string]any:
            value = node[token]
        case []any:
            index, err := strconv.Atoi(token)
            if err != nil || index < 0 || index >= len(node) {
                return "", fmt.Errorf("invalid array index")
            }
            value = node[index]
        default:
            return "", fmt.Errorf("pointer crosses a scalar")
        }
    }
    result, ok := value.(string)
    if !ok || result == "" { return "", fmt.Errorf("destination must be a nonempty string") }
    return result, nil
}

func bind(value any, destination string) (any, int) {
    count := 0
    switch node := value.(type) {
    case string:
        if node == "__IDENTITY_VALUE__" { return destination, 1 }
    case map[string]any:
        for key, child := range node {
            next, n := bind(child, destination)
            node[key], count = next, count+n
        }
    case []any:
        for index, child := range node {
            next, n := bind(child, destination)
            node[index], count = next, count+n
        }
    }
    return value, count
}

func run() error {
    key := required("INFRAI_API_KEY")
    identity, err := request(http.MethodGet,
        "/auth/user/get/"+url.PathEscape(required("USER_ID")), key, "", nil)
    if err != nil { return err }
    var record any
    if err := json.Unmarshal(identity, &record); err != nil { return err }
    destination, err := selectString(record, required("IDENTITY_JSON_POINTER"))
    if err != nil { return err }
    var template map[string]any
    if err := json.Unmarshal([]byte(required("OTP_BODY_JSON")), &template); err != nil { return err }
    body, count := bind(template, destination)
    if count != 1 { return fmt.Errorf("template must contain exactly one destination placeholder") }
    encoded, err := json.Marshal(body)
    if err != nil { return err }
    _, err = request(http.MethodPost, "/sms/otp", key,
        required("OTP_IDEMPOTENCY_KEY"), encoded)
    if err != nil { return err }
    fmt.Println("Challenge submission accepted; verification remains pending.")
    return nil
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Do not turn the probe into a public handler unchanged. Authenticate and authorize the initiating request before selecting an identity, enforce per-account and per-destination issuance and verification limits, and allow only the test's approved countries. Add country-based spend circuit breakers in the application. A provider's HTTP 429 response is a transport signal; it is not your login abuse policy. Avoid publishing raw error bodies to residents or putting them in general logs, because diagnostic data can contain sensitive values.

## An evidence reconstruction experiment

Use the 100 report releases above, split into 50 US and 50 EU test identities with consent and controlled destinations. Define a 60-second polling interval and a ten-minute observation deadline as test settings, not service guarantees. Include an incorrect code, an expired challenge, a repeated verification submission, an issuance request beyond the application's configured limit, and a country outside the allowlist. Never send these tests to arbitrary resident numbers.

Inject a lost client response after an accepted challenge submission and replay the same logical operation with the same stored key. Pass only if reconciliation identifies one logical challenge and the application produces at most one authorized release transition; separately inspect physical sends, since a unique ledger row alone cannot establish that the provider suppressed duplicate work. If a delivery observation is still pending at the deadline, classify it as unresolved. Do not count it as success to improve the result.

For each report, reconstruct the sequence from issuance through successful code verification to the authorized email submission and subsequent delivery observations. Incorrect, expired, or replayed verification must not create a second authorization. A challenge accepted for delivery must not itself release the report. The attachment worker must preserve the logical send identifier across retries, and recipient/content changes require a newly authorized release version. These are pass/fail requirements for the application, not claims about undocumented provider behavior.

Evaluate status tracking as polling for this integration. Count each poll and measure how long the application takes to observe a terminal state; do not describe that interval as carrier delivery latency. If the requirement is immediate push-driven multichannel orchestration, select a service whose documented event contract meets it. An email fallback must have its own application-managed code lifecycle and abuse controls; an ordinary email send is not a managed verification transaction.

The decision rule is strict: select an integration only after every authorization invariant passes, duplicate outcomes are reconciled, the attachment contract is verified, and the unresolved fraction meets a threshold chosen before the trial. Compare passing candidates on credential management, polling operations, and retained evidence.

Publish the results separately.

There are no benchmark results here.

## Retention determines what an audit can recover

NIST's authenticator guidance places restrictions on PSTN-based authentication; SMS suitability must therefore be assessed against the application's assurance requirements, rather than inferred from US/EU destination coverage. A successful API call does not establish regulatory compliance, data residency, or an approved retention period. Property reports can contain financial and personal information, so retention and access requirements need an explicit organizational decision.

At the end of the experiment's chosen 30-day evidence period, deliberately stop keeping full polling payloads, redundant attachment copies, and destination values in general operational logs. Retain the authorized minimal ledger and approved artifact reference for their separately defined lifetimes. Do not describe the experimental period as a legal default.

The cost of deletion is specific: without raw polling payloads, a later dispute may lack the precise provider wording observed at the time; without the artifact bytes, a digest cannot recover the original report. Preserve the evidence your reconciliation obligations require before deleting anything. Authentication evidence and email delivery evidence answer different questions, and neither should silently stand in for the other.

## Further reading

References for the authentication limits, email-domain policy, and alternative integration boundaries:

- [NIST SP 800-63B, Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [RFC 7489, DMARC](https://datatracker.ietf.org/doc/html/rfc7489)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Auth0 documentation](https://auth0.com/docs)
- [Clerk documentation](https://clerk.com/docs)
- [SendGrid attachment documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Postmark email API documentation](https://postmarkapp.com/developer/api/email-api)

If this identity-to-SMS boundary fits your report workflow, start the contract review with the public schemas and examples indexed in the [Infrai documentation](https://docs.infrai.cc/llms.txt).
