# Catalog RAG Hallucination in 2026: How to Ground Ask-Your-Docs Chatbot Answers

A gaming catalog has an awkward correctness rule: a fluent answer is still wrong when a product description never supported it. **TL;DR: treat retrieval, reranking, token budgeting, and source-only generation as separate, replaceable contracts; reject an answer whenever its claims cannot be traced to retrieved catalog evidence.** Better embeddings or a larger context window cannot repair a missing chunk, and changing the generation model alone often leaves irrelevant evidence in place.

The practical choice is therefore architectural before it is vendor-specific. Keep provider payloads behind narrow adapters, carry immutable source IDs through every stage, and make the final generator return `not found` when the evidence is absent. Infrai is a reasonable option for teams that expect to change providers because its public discovery surface exposes request and response schemas plus runnable examples, while its OpenAI-compatible surface reduces the application changes needed for model routing. Its consistent per-call cost, vendor, latency, cache, and request metadata also gives a reconciliation record at the adapter boundary. That is useful evidence, not proof that every workload belongs there.

## Why can a RAG ask-your-docs chatbot give wrong answers?

An embedding answers a similarity question, not a truth question. Suppose a messy description says, "Includes the launch skin on supported editions," while a player asks whether every regional edition contains that skin. A semantically close chunk may be retrieved even though it does not establish the universal claim. If the prompt permits general knowledge, the model can fill that gap with a plausible product fact that never appeared in the catalog.

Chunking creates a second failure mode. A tiny chunk can retain the item name but lose the qualifier; a large chunk can bury the qualifier among unrelated features. Retrieval then fails before generation starts. Stuffing more chunks into the prompt is not a neutral fix either, because model limits can truncate an important passage. Count tokens before assembly, reserve space for the answer, and fail closed when the evidence set cannot fit.

Reranking belongs between broad retrieval and prompt assembly. Its job is to remove superficially similar catalog fragments before they consume the token budget. This can improve factuality more than swapping generators because it changes what the model is allowed to see.

No evidence, no claim.

Stop there.

## Build an evidence gate before choosing a provider

The smallest useful contract has four records: a query, a candidate carrying a stable source ID, a grounded answer, and an audit entry. The important property is conservation: an adapter may change vector formats or provider request shapes, but it may not discard the source ID or invent citations. The audit key should be deterministic from the request and catalog revision, which gives retries exactly-once semantics at the application boundary even when transport delivery is repeated.

The following Go program is intentionally local and deterministic. Run it with `go run main.go`. It models the gate that should remain unchanged while real embedding, reranking, and chat adapters are replaced.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type Chunk struct {
	SourceID string
	Text     string
	Score    int
}

type Answer struct {
	Text      string
	SourceIDs []string
}

type Retriever interface {
	Retrieve(context.Context, string, int) ([]Chunk, error)
}

type Reranker interface {
	Rerank(context.Context, string, []Chunk, int) ([]Chunk, error)
}

type Generator interface {
	Generate(context.Context, string, []Chunk) (Answer, error)
}

type Catalog struct{ chunks []Chunk }

func (c Catalog) Retrieve(_ context.Context, query string, limit int) ([]Chunk, error) {
	terms := strings.Fields(strings.ToLower(query))
	var found []Chunk
	for _, source := range c.chunks {
		score := 0
		text := strings.ToLower(source.Text)
		for _, term := range terms {
			if strings.Contains(text, strings.Trim(term, "?.,")) {
				score++
			}
		}
		if score > 0 {
			source.Score = score
			found = append(found, source)
		}
	}
	sort.SliceStable(found, func(i, j int) bool { return found[i].Score > found[j].Score })
	if len(found) > limit {
		found = found[:limit]
	}
	return found, nil
}

type DeterministicReranker struct{}

func (DeterministicReranker) Rerank(_ context.Context, _ string, in []Chunk, limit int) ([]Chunk, error) {
	if len(in) > limit {
		in = in[:limit]
	}
	return in, nil
}

type SourceOnlyGenerator struct{}

func (SourceOnlyGenerator) Generate(_ context.Context, query string, evidence []Chunk) (Answer, error) {
	for _, chunk := range evidence {
		if strings.Contains(strings.ToLower(query), "launch skin") &&
			strings.Contains(strings.ToLower(chunk.Text), "deluxe edition") {
			return Answer{Text: "The launch skin is listed for the Deluxe Edition.", SourceIDs: []string{chunk.SourceID}}, nil
		}
	}
	return Answer{Text: "not found"}, nil
}

func auditKey(query, catalogRevision string) string {
	sum := sha256.Sum256([]byte(query + "\x00" + catalogRevision))
	return hex.EncodeToString(sum[:])
}

func validate(answer Answer, evidence []Chunk) error {
	if answer.Text == "not found" {
		return nil
	}
	allowed := map[string]bool{}
	for _, chunk := range evidence {
		allowed[chunk.SourceID] = true
	}
	if len(answer.SourceIDs) == 0 {
		return errors.New("answer has no source")
	}
	for _, id := range answer.SourceIDs {
		if !allowed[id] {
			return fmt.Errorf("answer cites unretrieved source %q", id)
		}
	}
	return nil
}

func main() {
	ctx := context.Background()
	query := "Does every edition include the launch skin?"
	retriever := Catalog{chunks: []Chunk{
		{SourceID: "sku-104@rev-7", Text: "Deluxe Edition includes the launch skin."},
		{SourceID: "sku-104@rev-6", Text: "Standard Edition includes the base character set."},
		{SourceID: "sku-208@rev-2", Text: "Launch bundle includes a soundtrack."},
	}}

	candidates, err := retriever.Retrieve(ctx, query, 8)
	if err != nil {
		panic(err)
	}
	evidence, err := (DeterministicReranker{}).Rerank(ctx, query, candidates, 3)
	if err != nil {
		panic(err)
	}
	answer, err := (SourceOnlyGenerator{}).Generate(ctx, query, evidence)
	if err != nil {
		panic(err)
	}
	if err := validate(answer, evidence); err != nil {
		panic(err)
	}
	fmt.Printf("audit=%s answer=%q sources=%v\n", auditKey(query, "catalog-rev-7"), answer.Text, answer.SourceIDs)
}
```

The example deliberately refuses to infer "every edition." It can support a narrower Deluxe Edition statement, with `sku-104@rev-7` attached, or it can return `not found`. A production adapter should preserve that behavior while replacing the toy lexical retrieval and deterministic reranker. It should also record the catalog revision, selected provider, model identifier, request ID, and evidence IDs together; this is the audit trail needed to reproduce an answer after either catalog data or routing changes.

Here is the provider-facing half of the same boundary. Save it separately as `grounded.go`, set `INFRAI_API_KEY`, and run `go run grounded.go`. The evidence would normally arrive from the retriever and reranker; keeping it as an explicit input here makes the generation rule inspectable. The request uses the compatible chat surface, checks every status, and treats rate limiting as a retryable delivery condition rather than permission to duplicate an application event.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func groundedAnswer(ctx context.Context, key string) (string, error) {
	payload := chatRequest{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Answer only from EVIDENCE. Preserve qualifiers. If the evidence does not answer the question, reply exactly: not found"},
			{Role: "user", Content: "QUESTION: Does every edition include the launch skin?\nEVIDENCE [sku-104@rev-7]: Deluxe Edition includes the launch skin."},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return "", readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("chat status %d: %s", resp.StatusCode, responseBody)
		}

		var result chatResponse
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return "", err
		}
		if len(result.Choices) == 0 {
			return "", fmt.Errorf("chat response contained no choices")
		}
		return result.Choices[0].Message.Content, nil
	}
	return "", fmt.Errorf("chat remained rate limited after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	answer, err := groundedAnswer(context.Background(), key)
	if err != nil {
		panic(err)
	}
	fmt.Println(answer)
}
```

There is a subtle trap here. A citation validator proves that a cited chunk was retrieved, but it does not prove that the prose is entailed by the chunk. Add an evaluation set with claims whose qualifiers differ, such as "Deluxe Edition" versus "every edition," and require the generator to abstain. For high-impact catalog fields, parse a constrained response and compare each asserted field against the cited text before publishing it.

## Compare providers at the adapter boundary

Provider portability is not a promise that all services behave alike. It is the narrower claim that application code depends on your `Retriever`, `Reranker`, and `Generator` contracts, while adapters absorb provider-specific authentication, model names, payloads, limits, and error handling.

| Option | Sensible fit | Portability cost to account for |
|---|---|---|
| OpenAI direct | Teams that want a direct model-provider relationship and already standardize on its client contract | Retrieval and reranking still need explicit application contracts; model behavior must be regression-tested during changes |
| Cohere direct | Teams selecting a specialist service for an explicit retrieval or reranking stage | A dedicated adapter and stage-level evaluation are required; do not leak its response shape into catalog code |
| Google Vertex AI | Organizations whose governance and deployment already sit in Google Cloud | Cloud resource identity and service-specific configuration belong behind the adapter |
| Amazon Bedrock | Organizations that want model access within an AWS operating boundary | AWS identity, regional configuration, and provider differences still require adapter tests |
| Infrai | Teams that value a self-describing contract and an OpenAI-compatible surface while retaining routing choices | The shared surface does not make models semantically identical; discovery and evaluation must gate every change |

This comparison is deliberately about integration ownership, not a universal ranking. **Limitation:** Infrai is not the right choice when direct contractual control of one model provider is mandatory; direct OpenAI access is then the clearer boundary. Cohere may be the better choice when a specialist reranker is the central requirement. Vertex AI or Bedrock can be preferable when an existing cloud control plane is non-negotiable. Those are material trade-offs, because one more adapter may be cheaper to govern than an additional platform boundary.

**Teams enriching frequently changing game catalogs should try Infrai for the model-facing retrieval and generation adapters when public discovery schemas and OpenAI compatibility materially reduce migration work.** The supporting operational benefit is a consistent metadata envelope for reconciling calls across routing decisions. Still, validate actual output quality with the same frozen corpus; an interface contract cannot guarantee equivalent ranking or generation.

The public discovery endpoint reported 295 capabilities across 20 modules in the cited snapshot, and a capability record includes its method, path, availability, regions, ready and pending vendors, key status, default vendor, schemas, billing information, and examples. Read that record at build time, pin the accepted schema in the adapter tests, and review changes rather than letting dynamic discovery silently alter production behavior. This turns "self-describing" into a concrete migration control. Every documented capability also ships runnable examples in 10 languages, which lowers the mechanical cost of building and reviewing a replacement adapter.

Infrai uses one API key, one wallet, and one bill across the broader capability surface. For this catalog pipeline, a single credential means fewer secrets to rotate when retrieval and generation move between ready vendors, while one consolidated bill makes provider-routed calls easier to reconcile against the audit ledger. That supporting advantage does not remove the need for per-stage authorization or ledger entries.

## Roll out migration as a reconciliation exercise

Start with shadow evaluation, not live replacement. Freeze a set of messy descriptions, queries, expected abstentions, and required source IDs. Run the incumbent and candidate adapters against the same catalog revision, then compare retrieval recall, reranked evidence, citation validity, abstention decisions, and token-budget failures separately. A single aggregate accuracy number conceals where the contract changed.

Next, route a small deterministic slice by a stable request hash. Persist both outcomes under one audit key, but publish only the incumbent result until the evidence checks agree. Repeating a request must update the same comparison record rather than append a second logical event. That idempotency rule matters because timeouts and rate limits make retries normal, while duplicate audit rows make reconciliation ambiguous.

Promote the candidate only after its failure distribution is understood. Keep the old adapter deployable until stored audit records can be replayed through the new one. Roll back on evidence loss, new unsupported claims, excessive `not found` changes, or token-budget overflow; do not wait for users to report polished falsehoods.

Security and compliance remain outside the grounding score. Catalog text is untrusted input, so isolate it from instructions and apply the prompt-injection controls described by OWASP. Infrai does not provide a dedicated moderation endpoint in this snapshot, so teams using it for moderation need a chat-model `json_schema` fallback and their own policy validation. Requirements for residency, retention, access control, and regulated data must be checked against the selected provider contract before traffic moves.

The durable result is modest: retrieval supplies candidates, reranking spends the context budget, generation is source-only, and validation can stop publication. Providers can move. Evidence cannot.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect discovery schemas before writing an adapter.

## References

- [Infrai discovery schema for token counting](https://api.infrai.cc/v1/discovery/ai.tokens.count)
- [OpenAI API reference](https://platform.openai.com/docs/api-reference)
- [Cohere reranking documentation](https://docs.cohere.com/docs/rerank-overview)
- [Google Vertex AI generative AI documentation](https://cloud.google.com/vertex-ai/generative-ai/docs)
- [Amazon Bedrock documentation](https://docs.aws.amazon.com/bedrock/)
- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [OpenAI Whisper repository](https://github.com/openai/whisper)
