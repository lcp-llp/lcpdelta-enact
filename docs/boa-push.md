# GB BOA changes

`BoaDPSHelper` receives cleaned actual accepted and on-table bid/offer changes through one SignalR connection, one subscription and one callback. Live GB Trade/Gold entitlement and the compatible backend are required.

## Subscribe

```python
import os
from threading import Event
from lcp_delta.enact import BoaDPSHelper

def on_update(batch):
    print(batch.date, batch.period, batch.revision)
    process_accepted(batch.accepted_bids, batch.accepted_offers,
                     batch.removed_accepted_bids, batch.removed_accepted_offers)
    process_on_table(batch.table_bids, batch.table_offers,
                     batch.removed_table_bids, batch.removed_table_offers)

with BoaDPSHelper(
    os.environ["LCPDELTA_ENACT_USERNAME"],
    os.environ["LCPDELTA_ENACT_API_KEY"],
    callback=on_update,
    callback_queue_maxsize=1000,
) as stream:
    stream.subscribe_to_boa_updates()
    Event().wait()
```

`process_accepted` and `process_on_table` stand for application code. Consumers can use only the collections they need. In asyncio applications use `async_subscribe_to_boa_updates()`, `async_unsubscribe_from_boa_updates()` and `async_close()`. Construct with `auto_connect=False` to defer network activity until subscribing. `unsubscribe_from_boa_updates()` stops the whole feed; closing the helper also stops it. The runnable example is `examples/boa_client.py`.

## Window and delivery

The backend maintains 48 half-hour groups: the current settlement period and the following 47 periods. The window begins at the current period's start and excludes the period starting exactly 24 hours later. It is a fixed 24-hour UTC duration across GB clock changes, rather than a GB calendar day. At 12:00 UTC on 11 November 2026, it covers period 25 that day through period 24 on 12 November. Python does not calculate or roll these groups. Historical periods leave coverage as the window advances, so later corrections to them are not received.

The helper starts with future changes only, with no initial snapshot, replay or automatic HTTP fetch. Restoration resumes future delivery; disconnects, queue failures and server drops can lose updates. Revisions belong to each settlement date/period, can skip or reset, and are source metadata rather than delivery guarantees. The helper does not deduplicate or discard batches based on revisions.

Callbacks accept one `BoaPush`, synchronously or asynchronously, and run sequentially. Synchronous callbacks use a dedicated worker thread; asynchronous callbacks run on the helper's background event loop. Keep callbacks fast and close the helper outside its callbacks. A positive `callback_queue_maxsize` bounds queued plus running callbacks and applies backpressure; zero is unbounded. Callback failures are logged without row contents and are not retried. Already queued batches may finish after unsubscribe. `close(wait_for_callbacks=True)` drains work during orderly shutdown.

## Cleaned callback contract

`BoaPush` exposes `date` as `datetime.date`, `period`, `revision`, and these tuples of public row dictionaries:

| Changed rows | Removed rows |
| --- | --- |
| `accepted_bids` | `removed_accepted_bids` |
| `accepted_offers` | `removed_accepted_offers` |
| `table_bids` | `removed_table_bids` |
| `table_offers` | `removed_table_offers` |

An on-table-only batch invokes the same callback. Collections are deltas, not replacement snapshots. Apply removals before changed rows and key records by settlement date, period, accepted/on-table collection, bid/offer side and `deltaId`. A correction can remove one identity and insert another in the same batch.

Python projects raw frontend rows into the established public fields:

| Fields | Meaning |
| --- | --- |
| `deltaId` | Preserve the source row identity verbatim. |
| `plantId`, `bodId`, `isBid`, `isAccepted` | Plant/BOD identifiers and Boolean side/status flags. |
| `volume`, `bidPrice`, `offerPrice`, `cost` | Existing public BOA units and signs; values are preserved. |
| `soFlag`, `storFlag`, `cadlFlag` | Boolean BOA classification flags. |
| `bidOnTableTypeEnum`, `fuelType` | Public classification values. |
| `sel`, `mzt`, `ndb`, `ndo`, `ndz`, `mnzt` | Nullable operating parameters. |
| `coords` | Rounded to three decimals, midpoint away from zero; unavailable coordinates are `[0.0, 0.0]`. |
| `additionalBsadData` | BSAD public `plantId`, `partyName`, `fuelType`, or null. |
| `acceptedTime` | Nullable source acceptance timestamp, mapped from frontend `timeAccepted`. |

Frontend display/detail fields are not exposed. Rows marked `theoretical` or `nonBmPrediction` are ignored in every changed and removed collection. Ignoring a prediction never synthesizes a removal or modifies a previously delivered actual row. Only explicit actual-row removals are delivered. The helper does not retain row history.

Frontend `reloadRequired` and `previousRevision` are deliberately ignored, omitted from callback objects, and do not cause warnings, revision-gap processing or recovery calls. If filtering leaves no actual rows, including a reload-only message, there is no callback. If actual changes accompany the flag, those changes are still delivered.

## Connection lifecycle

At INFO level, the helper logs connection opening, successful subscription, restoration, unsubscribe and shutdown. An unexpected disconnect logs a warning. Restoration success is logged only after server confirmation and notes that missed updates are not replayed.

The engine broadcasts `heartbeat` every five seconds. While connected and subscribed, a missing heartbeat logs one warning after `heartbeat_timeout_seconds` (default 30), followed by one INFO message on recovery. Monitoring uses local monotonic receipt time. Disconnect, unsubscribe and shutdown stop monitoring; reconnect starts a fresh grace period. Use a positive finite timeout or `None` to disable. A stale heartbeat does not trigger reconnects or HTTP fetches and does not prove individual BOA batches were missed.

The helper ignores known unrelated `zeroPnUpdate`, `earliestDateInBuffer` and `nonBmStorUpdate` events. Other unhandled-event warnings remain visible. The example displays accepted/on-table changes with settlement date, period and revision. Its `Flagged` column uses SO/STOR/CADL flags. Row IDs remain available in the callback but are not printed. Its UTC received timestamp is local callback time and can be delayed by queued work.

## Backend wire subscription

Non-Python consumers authenticate normally, connect to `/dataHub`, register `boaDashboardUpdated`, then invoke `JoinBoaPush()` with no arguments. A successful join returns `data.pushName = "boaDashboardUpdated"`; structured API errors remain in `messages`. Use `ReconnectToPush("boaDashboardUpdated")` to restore and `LeaveGroup("boaDashboardUpdated")` to unsubscribe. If the DPS usage window expired, perform a fresh validated join.

The raw envelope is `{ "datePeriod": { "date": "2026-11-06T00:00:00", "period": 24 }, "item": { ... } }`. `item` contains the eight camel-case row collections, `revision`, and frontend metadata. The backend publishes this shared payload once; the Python public projection happens locally. There is no separate accepted-only publication.

## Deployment and verification

Deploy the shared-delta backend from Enact PR 80581, then the SignalR host from PR 83869 with the 24-hour window, before using this helper. Unit tests mock credentials and transport. The integration test checks authorized subscribe/unsubscribe and requires a compatible deployment and credentials; event-delivery testing additionally needs a controlled update. Leave `ENACT_ENV` unset unless a maintainer explicitly asks for staging.
