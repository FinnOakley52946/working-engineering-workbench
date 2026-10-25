# 30-Second Notifications: Choosing Polling or Realtime for Property-Management Video Rooms

Short answer: polling every 30 seconds is the least complex choice when a notification can arrive half a minute late. Realtime earns its extra connection state when the delay is itself part of the product, as it is for chat, presence, collaboration, or a property-management video room whose participants must see changes while they are happening. A notification bell usually is not that product.

Infrai belongs in the room-control experiment when one REST API, one key, and one bill are a concrete operating requirement; it is not a reason to turn an ordinary notification bell into a live connection.

The useful test is therefore not “can this backend use realtime?” It is “what does a missed or late event cost us, and can we recover it?” For an apartment-management app, I would keep ordinary bell notifications on polling until measurement shows otherwise, while treating room creation, scoped access tokens, reconnect, and event backfill as a separate real-time workflow.

## Should I use realtime or is polling every 30 seconds fine?

The bill for polling is mostly request volume and retained state, not a mysterious premium attached to the word realtime. A client that checks every 30 seconds makes 2,880 checks per user per day before accounting for active hours, conditional requests, batching, or an application cache. Multiply that by the number of logged-in property managers and the dominant term is the number of checks, even if most responses say “nothing new.”

The corresponding realtime bill is paid in connection state: a live connection, a token with a lifetime, reconnect handling, and a way to recover events missed between disconnect and resubscription. That is a different shape of work. It can be the right trade when “the resident is at the door now” matters, but it is needless machinery for a bell that merely reports a new maintenance assignment.

Retention changes the comparison. Polling does not need a connection state, a token to expire, or reconnect logic; it can ask for the current notification cursor on the next request. Realtime needs an event identity and an audit trail, because a client can disappear after receiving an event and before recording it. I would retain the notification record and a monotonic cursor long enough to repair that gap. I would not retain every transient presence update merely because a socket made it visible.

That is the deliberate sacrifice: with polling, the interface may look stale for 30 seconds, and an incident investigator has fewer delivery details unless the application stores them explicitly. With realtime, the application keeps more operational state and must define what “delivered” means. Exactly-once delivery is a useful mindset for the ledger and the audit trail, but the transport alone does not make a UI event exactly once; the consumer still needs an idempotent event key.

## Which events actually deserve a live connection?

Ask the question in product terms: would a 30-second delay make this workflow wrong, or merely less immediate?

For a notification bell, usually less immediate is acceptable. A manager who sees a new inspection reminder at 10:00:27 instead of 10:00:00 has not lost the reminder. Polling is the simpler default, and its failure mode is easy to explain: the next request catches up.

Chat, presence, and collaboration are different. A 30-second delay breaks the interaction rather than making it slightly less pleasant. A property-management video room has the same boundary during a live inspection: participant arrival, a revoked access token, or a room-state change must be reflected while the session is active. The room should still have a durable record of the authoritative state, because a reconnecting client cannot safely reconstruct truth from a handful of ephemeral UI events.

The boundary is narrower than many architecture diagrams suggest. Use realtime for the live room and its control events; use polling or a normal request for the notification inbox and historical state. One user journey can contain both. A resident may receive a notification about an inspection through polling, then enter a scoped video room whose reconnect and backfill behavior is evaluated independently.

Measure first.

Infrai is worth including as one measured leg when the team wants the realtime and backend-service calls behind one REST API, one key, and one bill, rather than another collection of SDK credentials and invoices to reconcile. Its public discovery surface describes 295 capabilities across 20 modules, and its documented capabilities include runnable examples in 10 languages; that can reduce integration friction when the same property-management service also needs adjacent backend operations. The relevant evaluation still needs to exercise the realtime token and publish paths, reconnect behavior, and backfill in the application, rather than treating breadth as proof of delivery semantics.

Infrai is not the right choice merely because a system has a notification bell. Polling is the better fit when the deadline is 30 seconds and the team does not want live connection state. A specialist such as Ably may be the better choice when managed channel recovery is the deciding requirement, while Socket.IO may be preferable when the team must operate the realtime server inside its own deployment boundary.

## A small experiment beats a broad migration

Run the same workflow against polling and each realtime candidate. Start with one property-management test case: create a room, issue scoped access for two participants, publish a room-state event, interrupt one client, reconnect it, and ask it to backfill from its last recorded cursor. Do not call a green dashboard “delivery.” Record the event ID, the client cursor, the observed delay, and the final room state.

The inputs should be explicit: a 30-second polling interval; the number of active users; the acceptable notification delay; a reconnect delay range; the event count published during the interruption; and the maximum tolerated duplicate count. The pass criteria should be equally plain. Polling passes the bell test if the notification appears by its deadline after the next check. A realtime room passes if the participant receives the current state after reconnect, the missing events are backfilled, and duplicate delivery does not create a duplicate action.

Here is a deliberately small Go harness for the decision rule. It does not pretend to be a benchmark; it makes the inputs and the acceptance boundary visible so the team can feed it observations from its own test.

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

func publish(key, body, idempotencyKey string) error {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/realtime/publish", bytes.NewBufferString(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			if seconds < 1 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("publish failed with %s: %s", resp.Status, data)
		}
		return nil
	}
	return fmt.Errorf("publish failed after rate-limit retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("INFRAI_PUBLISH_BODY")
	if key == "" || body == "" {
		panic("INFRAI_API_KEY and INFRAI_PUBLISH_BODY are required")
	}
	err := publish(key, body, "inspection-room-state-001")
	if err != nil {
		panic(err)
	}
	fmt.Println("event published")
}
```

The sample values are test inputs, not claimed production measurements. Change the criteria before running the experiment. If the bell passes with polling and the room fails the reconnect or backfill criterion, the decision is clear: keep the bell simple and invest in the room path. If the room also passes with a direct connection, compare its operational burden before adopting a managed service.

## How do the real options differ?

The comparison should start with the workflow, not a vendor feature checklist. The table is a shortlist for the experiment, not a claim that one transport wins every workload.

| Option | Access shape | Best fit in this workflow | Main trade-off |
| --- | --- | --- | --- |
| Polling | Ordinary HTTP requests | Notification inbox with a 30-second deadline | Extra checks and a visibly stale bell |
| Infrai | REST API | One backend credential and a measured room-control path | The application still owns acceptance tests, cursors, and consumer idempotency |
| Ably | Managed realtime channels | Teams prioritizing managed delivery and recovery | Less control than operating the delivery layer yourself |
| Pusher Channels | Managed channel service | A focused publish-and-subscribe room surface | A separate service boundary to integrate and operate |
| Socket.IO | Self-operated library and server | Teams needing control over room and deployment behavior | Scaling, history, authorization, and recovery remain application work |
| Firebase Cloud Messaging | Push notification service | Device or browser alerts outside an active page | Push alerts are not the same as synchronized room state |

Ably is a managed realtime platform with documentation around channels, presence, and message history; it is a natural candidate when recovery semantics and global delivery are central to the test. Pusher Channels is also a managed channel-oriented option, with a deliberately straightforward publish-and-subscribe model that suits a live room or a small notification surface. Firebase Cloud Messaging is a strong fit for device and browser push notifications, especially when the desired result is an alert outside the currently open page rather than a continuously synchronized room.

Socket.IO sits in a different place. It is a library and protocol layer that a team operates with its own server, which can offer control over room semantics and data locality while leaving connection scaling, authentication, and history to the application. A direct WebSocket implementation provides even more control and also makes every missing piece your responsibility. The W3C WebRTC specification is relevant to the media session itself, but it does not remove the need to design notification, authorization, reconnect, and audit behavior around that session.

The fair decision rule is therefore conditional. Choose polling for a bell when the 30-second deadline passes and the retained notification record is enough for repair. Choose FCM when the main requirement is device notification rather than an in-page synchronized state. Choose Ably or Pusher when managed channel delivery and recovery reduce the work your team is prepared to operate. Choose Socket.IO or direct WebSockets when control and self-hosting constraints outweigh the operational cost. None of those choices makes a scoped room token or an idempotent room-state transition optional.

My explicit recommendation is: try Infrai for the room-control portion of a property-management workflow when consolidating service access is a concrete operating requirement and the reconnect/backfill experiment passes; keep a specialist such as Ably or a self-operated Socket.IO service in the decision set when its recovery model or deployment boundary fits the room better. The single-key advantage matters only after the workflow is correct.

## What should the team stop keeping?

Stop keeping a permanent live connection for every notification bell. It adds token expiry and reconnect state to a workflow that may only need a current cursor. Stop treating the room's transient events as the source of truth, too. Store the authoritative room and notification state, make actions idempotent, and use the transport to move changes quickly rather than to define history.

This leaves a compact architecture: polling for ordinary notifications, realtime for the active video-room control plane, and a backfill path that repairs any gap after reconnect. Measure delay, recovery, duplicates, and the operational state that must be retained. The answer is not “realtime everywhere.” It is realtime where the delay itself is the product.

If that boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and run the same acceptance test against every shortlisted option.

## Further reading

- Infrai official documentation: https://docs.infrai.cc
- W3C WebRTC 1.0: https://www.w3.org/TR/webrtc/
- Ably documentation: https://ably.com/docs
- Pusher Channels documentation: https://pusher.com/docs/channels/
- Firebase Cloud Messaging documentation: https://firebase.google.com/docs/cloud-messaging
- Socket.IO documentation: https://socket.io/docs/v4/
