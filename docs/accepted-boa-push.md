# Accepted GB BOA changes

`AcceptedBoaDPSHelper` subscribes to future accepted GB bid/offer changes through the public Enact SignalR `/dataHub`. It requires the same live GB Trade/Gold entitlement as the public BOA API. One subscription covers all settlement periods currently published by the live engine, including corrections to retained periods. There is no date/period filter.

## Python

```python
import os
from threading import Event
from lcp_delta.enact import AcceptedBoaDPSHelper

def on_update(batch):
    print(batch.date, batch.period, batch.revision)
    for row in batch.removed_accepted_bids:
        remove_bid(batch.date, batch.period, row["deltaId"])
    for row in batch.accepted_bids:
        replace_bid(batch.date, batch.period, row["deltaId"], row)
    # Handle removed_accepted_offers and accepted_offers in the same way.

with AcceptedBoaDPSHelper(
    os.environ["LCPDELTA_ENACT_USERNAME"],
    os.environ["LCPDELTA_ENACT_API_KEY"],
    callback=on_update,
    callback_queue_maxsize=1000,
    heartbeat_timeout_seconds=30,
) as stream:
    stream.subscribe_to_accepted_boa_updates()
    Event().wait()
```

`remove_bid` and `replace_bid` represent application code. The runnable example is `examples/accepted_boa_client.py`. In an asyncio application use `async_subscribe_to_accepted_boa_updates()`, `async_unsubscribe_from_accepted_boa_updates()`, and `async_close()`. Construct with `auto_connect=False` to defer network activity until subscribing.

The runnable example displays each batch as a table with settlement date, period, revision and full row IDs. Its UTC "Received at" timestamp records when the local callback begins; queued work can delay it beyond network receipt. It is separate from each row's source `acceptedTime`. Replace this example display callback with your own handling of the raw batch.

At INFO level, the helper logs connection opening, successful subscription, restoration start/success, subscription stop and connection shutdown. An unexpected disconnect logs a warning. Restoration success is logged only after the server confirms the subscription, and notes that missed updates are not replayed. Enable INFO logging as the example does to see these lifecycle messages.

Callbacks accept one `AcceptedBoaPush` and may be sync or async. Each source batch results in one callback, containing four tuples of public row dictionaries. Callbacks run sequentially; synchronous callbacks run on a dedicated worker thread, while asynchronous callbacks run on the helper's background event loop. Do not assume the callback runs on the application's event loop. Keep callbacks fast and close the helper from outside its callbacks.

A positive `callback_queue_maxsize` bounds queued plus running callbacks and applies receive backpressure. Zero is unbounded. Callback failures are logged without row contents; failed callbacks are not retried. Unsubscribe stops future delivery; already queued batches may finish. `close(wait_for_callbacks=True)` drains queued work during orderly shutdown.

The engine broadcasts `heartbeat` every five seconds. While subscribed and connected, the helper logs one warning if no heartbeat arrives for `heartbeat_timeout_seconds` (default 30 seconds), then one INFO message when receipt resumes. It checks at intervals of at most five seconds using local monotonic receipt time, independently of the server timestamp. Disconnect, unsubscribe and shutdown stop monitoring; reconnect starts a fresh grace period. Set a positive finite timeout to customize it, or `None` to disable monitoring. Healthy heartbeats do not produce log messages or BOA callbacks.

An overdue heartbeat means engine-to-client delivery may be stale, even if SignalR remains connected. It does not prove the transport is disconnected or detect missed BOA batches, and does not trigger a reconnect or HTTP fetch.

The helper ignores the server-wide `zeroPnUpdate` broadcast, which tells frontend plant details to refresh last-off/zero-PN information, and `earliestDateInBuffer`, which tells frontend charts the earliest date available in the live buffer for choosing live versus historical requests. These broadcasts do not affect BOA callbacks or heartbeat monitoring. All three known events have handlers, so they produce no missing-handler warnings. Other unhandled-event warnings and connection errors remain visible.

## Public wire contract

For non-Python consumers, authenticate with the usual public API bearer token, connect to `/dataHub`, and register a handler for `acceptedBoaUpdated` before invoking `JoinAcceptedBoaPush` with no arguments. A successful join returns the usual API envelope containing `data.pushName: "acceptedBoaUpdated"`. Registering a handler alone does not authorize or join the feed. Structured API errors remain in `messages`.

The group and event are both `acceptedBoaUpdated`. Call `LeaveGroup("acceptedBoaUpdated")` to unsubscribe. On reconnect, invoke `ReconnectToPush("acceptedBoaUpdated")`; an expired subscription window requires another validated `JoinAcceptedBoaPush`. A successful fresh join uses the standard DPS subscription increment once per user/subscription usage window. Receiving individual batches does not create additional joins.

```json
{
  "date": "2026-11-06",
  "period": 24,
  "revision": 105,
  "acceptedBids": [],
  "acceptedOffers": [],
  "removedAcceptedBids": [],
  "removedAcceptedOffers": []
}
```

All four arrays are present. The empty arrays above illustrate the shape; the publisher sends a batch only when at least one accepted collection is nonempty. New and corrected rows appear in `acceptedBids`/`acceptedOffers`. Removed collections contain the previous public rows. These are changes, not complete replacement lists. On-table rows, frontend reload instructions, and dashboard metrics are excluded.

`date` is the GB settlement date (`YYYY-MM-DD`), `period` is the settlement period for that date (including 46/50-period clock-change days), and `revision` is the source dashboard revision for that date/period. Python exposes the date as `datetime.date` and the four collections as `accepted_bids`, `accepted_offers`, `removed_accepted_bids`, and `removed_accepted_offers`.

Each row uses the existing public BOA projection, with `deltaId` and nullable `acceptedTime` added:

| Fields | Meaning |
|---|---|
| `deltaId` | Existing source row identity; preserve verbatim. |
| `plantId`, `bodId`, `isBid`, `isAccepted` | Plant, BOD and row side/status. |
| `volume`, `bidPrice`, `offerPrice`, `cost` | Values in the same units and sign conventions as the public BOA API. |
| `soFlag`, `storFlag`, `cadlFlag` | Existing BOA classification flags. |
| `bidOnTableTypeEnum`, `fuelType` | Existing public classification values. |
| `sel`, `mzt`, `ndb`, `ndo`, `ndz`, `mnzt` | Existing nullable public operating parameters. |
| `coords`, `additionalBsadData` | Existing public location and BSAD metadata; BSAD metadata may be null. |
| `acceptedTime` | Source acceptance timestamp when available; may be null. |

The row fields follow the existing public BOA projection, but this feed contains actual accepted rows only. Rows marked `NonBmStorPrediction` or `TheoreticalPredicted` are excluded from both previous and current lists before comparison. If a row becomes predicted, its previous actual entry is removed; if it becomes actual, it is added. Frontend prediction updates remain unchanged. The public row does not expose raw frontend prediction/detail fields.

Identify records using settlement date, period, bid/offer side, and `deltaId`. A volume correction keeps the ID; a price or flag correction can change it. In that case the same batch contains removal of the old ID and insertion of the new ID. Apply removals before changed rows when updating local state.

## Delivery semantics

The helper starts with future events only. It supplies no initial snapshot, replay, automatic HTTP fetch, or reload/recovery instruction. Connection restoration resumes future delivery; changes during a disconnect can be missed. Retries or connection changes can also cause duplicate delivery. Source ordering is preserved through the existing dispatch partition and sequential Python callback queue, subject to the existing server queues' overflow/drop and retry policies.

Revision numbers can skip because on-table-only changes are not published here, and can reset when engine state is rebuilt. They are not durable event IDs or evidence of consecutive delivery. The helper does not deduplicate or reject events based on revision. Consumers may independently call the existing BM HTTP methods; this release does not turn them into a revision-aware snapshot/recovery protocol.

## Deployment and validation

Deploy the compatible SignalR host and BOA engine/buffer backend (based on Enact PR 80581) before using Python package 1.1.10. This document is the repository contract draft for transfer to the maintained API documentation; publishing that external documentation is separate.

Unit tests use mocked credentials/SignalR. The integration subscription test requires a deployed compatible backend and an account with live GB BOA entitlement. Testing event receipt additionally requires a controlled acceptance change; do not infer event delivery merely from a successful join. Leave `ENACT_ENV` unset unless a maintainer explicitly requests staging.
