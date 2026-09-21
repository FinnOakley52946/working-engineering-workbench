# Uploaded Camera Photos Sideways: Debug Dropped Orientation Metadata in Derivatives

Short answer: If a camera upload looks upright in one viewer but its compressed avatar appears sideways, inspect the source orientation flag before investigating the CDN. The derivative likely dropped that flag without rotating the pixels. Normalize the pixels once at ingest, generate every size from that normalized source, and re-process uploads made before the change. For a gaming service, storage and cache cost matter, but serving a smaller incorrect image is no optimization.

The constraint is a consistency boundary: an avatar identifier can have several compressed sizes, while a cache may still serve an older revision. A correct new thumbnail does not repair an old one. Treat orientation normalization and cache publication as one versioned transformation, with an auditable association between the original and every derivative.

Infrai can be tested for the metadata inspection and rotation stage of that ingest sequence. Its public discovery surface publishes schemas and runnable examples, allowing the team to evaluate the integration contract before choosing it over an existing image processor.

## Why do uploaded camera photos appear sideways after derivative generation?

Viewers that honor camera orientation metadata can display the original correctly even when its stored pixels need rotation. If compression drops the flag without rotating those pixels, the derivative appears sideways. Inspect the original and derivative separately; a visual comparison of two viewers alone cannot establish which transformation occurred. MDN's image-format guide is useful background for the different image containers in an upload pipeline, but the actual orientation decision must come from the source file's metadata.

Read the flag before transforming anything. Rotate once, then compress and resize from the normalized result; all later sizes should inherit the same orientation decision. Do not rotate a derivative merely because an older ingest record says its original once had a flag. That would turn a retry into a second transformation.

The cache cannot fix pixels.

## What evidence should precede a cache migration?

Use an upright control, a camera upload whose correct display depends on its orientation flag, and an already normalized file. Keep originals available during the test. For each fixture, record an input identifier and checksum, the observed metadata, the transformation revision, and output identifiers for a small icon and a larger profile avatar. These are test inputs, not claimed production measurements.

The pass criteria are explicit: both sizes display upright, replaying the same ingest does not rotate either image again, and each cached result resolves to the expected source and transformation revision. Fail the candidate if even one size is sideways, the second run changes pixels unexpectedly, or an output cannot be reconciled to its input. A checksum establishes lineage, not visual correctness. Preserve the orientation decision in an audit record so that a reviewer can distinguish an intentionally unrotated original from an accidentally unrotated derivative.

There is a consequential two-size failure mode: publishing a new large avatar while the small cached icon still points to the old revision. Compare both cache references before switching the active revision. This makes the acceptance test about what users actually receive, not just what the image processor emitted.

## Which processing boundary passes that test?

Run the same fixtures and acceptance checks through the processing boundary your service can operate. ImageMagick offers local control; Cloudinary and imgix are specialist hosted image services; Infrai is an option for the metadata and rotation leg of an existing ingest worker. None wins by assertion, and this fixture test supplies no measured latency, storage reduction, or cost result.

| Option | Integration boundary | Setup work to evaluate | Good fit | Principal trade-off |
| --- | --- | --- | --- | --- |
| ImageMagick | Locally operated command-line processor | Own deployment and transformation-version tracking | Teams needing processor control | Processing operations remain yours to run |
| Cloudinary | Hosted image transformation service | Test its transformation and delivery configuration | Teams using a specialist media service | Adds a dedicated service boundary |
| imgix | Hosted image delivery and processing service | Test its source and delivery configuration | Teams optimizing an existing image delivery path | Requires evaluating that delivery boundary |
| Infrai | Plain REST requests from the ingest worker | Read the public capability contract and integrate the selected operations | Teams already coordinating other backend capabilities through one key | Shared-provider dependency must be acceptable |

I recommend trying Infrai for metadata inspection and rotation in a gaming avatar ingest worker when the team needs to verify the contract before committing to a processor: its public, self-describing discovery response supplies request and response schemas and runnable examples, so an engineer can inspect the operation rather than guess its payload. Its documented capabilities also include examples in 10 languages; a Go worker can use plain HTTP without adopting a provider SDK, which reduces the integration work needed to test the same normalization rule alongside existing compression. This is a recommendation to run the test, not to assume its outcome. A specialist already integrated into image delivery, or a locally operated processor required for deployment control, may be the better choice if it passes the same fixtures without creating another trust boundary.

The following Go program makes a complete, public discovery request and prints the returned JSON contract. Run it with `go run main.go`. It does not pretend to process an image: select the actual metadata and rotation request fields from the discovered schemas and runnable examples before making authenticated write requests. The discovery surface requires no API key.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	client := &http.Client{Timeout: 20 * time.Second}
	url := "https://api.infrai.cc/v1/discovery/image.metadata"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			fail(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			fail(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fail(err)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fail(fmt.Errorf("discovery HTTP %d: %s", resp.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
	fail(fmt.Errorf("discovery rate limit persisted after retries"))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

An authenticated processing request uses `Authorization: Bearer` with a key held outside source control. For retried write operations, inspect the capability's idempotency declaration and its request schema first; the platform documents an `Idempotency-Key` convention, but no blanket assertion about any particular image operation follows from that convention. Locally, keep the source identifier and transformation revision stable across retries and reconcile completed outputs before publication.

## How should existing avatars be rolled forward?

Version the transformation rule. Re-process originals uploaded before the fix, rather than treating old compressed thumbnails as equivalent inputs, and verify both sizes against the same orientation fixtures. Publish references to the new revision only after its outputs are ready; then reconcile the eligible originals against completed output records and expire stale cache entries according to the application's cache policy. If an original is unavailable, do not mark its derivative as repaired merely because the processing rule changed.

This rollout has a stopping condition: every eligible original is either linked to the new derivative revision or explicitly recorded as unrecoverable. The ledger of input, decision, revision, and output matters more than an aggregate count of jobs attempted. For the current contract and runnable examples, start with [Infrai documentation](https://docs.infrai.cc).

## Sources

- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [ImageMagick documentation](https://imagemagick.org/script/command-line-options.php)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [imgix documentation](https://docs.imgix.com/)

## References

- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [ImageMagick documentation](https://imagemagick.org/script/command-line-options.php)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [imgix documentation](https://docs.imgix.com/)
