# Accepted BOA changes through the public API and Python package

## Overview

Expose changes to actual accepted GB bids and offers through an authenticated public SignalR subscription and a dedicated Python helper. Reuse the accepted-row comparison added by Enact PR [80581](https://dev.azure.com/lcpdevelop/Enact/_git/Enact/pullrequest/80581) on snapshots filtered to exclude predictions, projecting its results into an explicit external API contract alongside frontend publication.

One subscription receives all live GB accepted-row changes, including corrections for settlement periods still held by the live engine. Each push contains all changed and removed accepted rows from one source update, its GB settlement date and period, and the existing source dashboard revision. Preserve the frontend update's batching, projecting only its accepted-row collections into the public contract. Revisions can skip because of dashboard changes that do not affect accepted rows. Revision is source metadata, not a consecutive delivery sequence.

This is a live event feed. It sends no initial snapshot, performs no automatic HTTP fetches, and offers no replay or delivery guarantee across disconnects. Frontend `reloadRequired`/recovery instructions, on-table rows, map notifications, market metrics, historical export, additional filters, and a new snapshot/recovery API are outside scope. Keep existing frontend pushes and existing Python APIs compatible.

### Repositories and evidence

- Backend: `C:/Users/Patrick.Courts/Source/repos/Enact`.
- Python package and this plan: `C:/Users/Patrick.Courts/Source/repos/lcpdelta-enact`.
- Inspected PR source: `origin/codex/boa-signalr-snapshot`, commit `28a8ddc9927bf54969a2f8ee999d947c801db42c`. Refresh the PR status and source before implementation; work from a branch containing its accepted-row delta implementation.
- Backend entry points: `Enact.EngineApiBuffer/DataHolders/BidOfferDashboardDataHolder.cs` (`PublishCombinedDashboardUpdate`), `Enact.Components/Market/Plant/Front End/BidOfferDashboardData.cs` (`BidOfferDashboardDataUpdate`), and `Enact.SignalRNew/Hubs/DataHubNew.cs`.
- The source delta already separates changed and removed accepted bids/offers. `CreateReload()` emits no row changes, but the inspected production callers do not request that path. Do not add reload handling to the public feed or expand this work into frontend recovery.
- `MultiSeriesDPSHelper` consumes chart `SeriesPing` messages; the new row contract does not fit that parser or `JoinMultiSeries` validation/routing.
- The existing public BOA period endpoint requires live GB Trade/Gold access. Use that public access policy as the baseline, rather than treating frontend group membership as API authorization.
- Pre-existing Python working-tree changes were present in `lcpdelta_python_package/src/lcp_delta/enact/configs/base_endpoints_default.py`, and `AGENTS.md` was untracked. Backend `.worktrees/` was untracked. Preserve these and recheck status before implementation.

### Proposed external contract

Use an explicit camel-case message, for example:

```json
{
  "date": "2026-11-06",
  "period": 24,
  "revision": 105,
  "acceptedBids": [
    {
      "deltaId": "existing-source-delta-id",
      "plantId": "EXAMPLE_PLANT",
      "isBid": true,
      "isAccepted": true,
      "bodId": "-1",
      "volume": 10.0,
      "bidPrice": 50.0,
      "offerPrice": 60.0
    }
  ],
  "acceptedOffers": [],
  "removedAcceptedBids": [],
  "removedAcceptedOffers": []
}
```

The example row is illustrative, not an exhaustive schema. Define its complete allowlisted fields using the existing public BOA row projection (`BidOfferItemForFrontEndToSend` and, where appropriate, its accepted-time projection). Do not serialize the raw frontend row. Document units and nullability using the existing public BOA conventions; retain the source `deltaId` verbatim rather than reconstructing it from a reduced public row.

- `date` is a date-only GB settlement date, and `period` is its settlement-period number, including short/long clock-change days.
- `acceptedBids` and `acceptedOffers` contain complete current public rows to insert or replace. They contain only changed rows, not the complete accepted lists. `removedAcceptedBids` and `removedAcceptedOffers` contain complete previous public rows to remove. Include all four arrays, using empty arrays when appropriate; no per-row operation field is required.
- Each public row includes its existing `deltaId`. Identify a record by `(date, period, bid/offer collection, deltaId)` so the collections remain distinguishable.
- A volume correction retains its ID and produces an entry in a changed collection. Price/flag changes can change `deltaId`, putting the old record in a removed collection and the new record in a changed collection in the same message. Consumers maintaining state apply removals before inserting/replacing changed rows.
- Send one API message per source date/period update with accepted changes. Do not split the batch into separate row messages or merge different source updates. Suppress updates with all four accepted collections empty.
- Source revisions are scoped to a date/period and may reset when source state is rebuilt. They are not durable event IDs or proof of complete delivery.

## Tasks

- [x] **1. Establish implementation branches and freeze the wire contract.**
  - Refresh PR 80581 using the ADO workflow and locate a backend branch containing its changes. Read applicable repository instructions and `docs/AI_BACKEND_GUIDE.md`; preserve unrelated working-tree changes. Do not implement by checking out over active work.
  - Define the external batch DTO, exact public row fields including `deltaId`, four accepted-row collections, serialization, and a distinct API-only group/event. Proposed hub method: `JoinAcceptedBoaPush()`, returning the standard `data.pushName` response plus event information if the event is not identical to that name.
  - Keep this feed GB-wide with no required date/period arguments. It follows future publications for all live retained periods; it does not emit the current contents when joining.
  - Exclude rows marked `NonBmStorPrediction` or `TheoreticalPredicted` from both previous and current accepted snapshots before comparison. Reuse existing public BOA fields and entitlement checks for the remaining actual accepted rows.
  - Verify the contract with serialization examples for changed rows, removed rows, and a batch containing several changes/removals across both sides at one revision. Fix the expected contract in backend and Python fixtures.

- [x] **2. Project and publish accepted-row events at the existing backend publication point.**
  - In or immediately alongside `PublishCombinedDashboardUpdate`, use the existing accepted-row comparison on previous/current snapshots filtered to actual rows. Keep the frontend delta and its prediction rows unchanged. Filtering the finished frontend delta alone is insufficient for actual/predicted status transitions sharing an ID.
  - Map those collections to one public batch DTO and send one event to the API group when at least one accepted collection is nonempty. Preserve the source date/period/revision and batch boundary. Publish once per source update, not once per row or once for each frontend forecast/non-forecast group.
  - Do not add another SignalR listener, another market-data comparison engine, or a per-client row cache. Keep the existing source revision.
  - Reuse the existing engine-to-hub dispatch path (`InMemoryBuffer.SendUpdateToGroup` and existing hub dispatch). Preserve order for one date/period without introducing blocking network waits inside the data-holder lock. Verify the dispatch queue's actual ordering before relying on it.
  - Verify with focused backend tests: new accepted bid/offer, same-ID correction, ID-changing correction, removal, several changes/removals in exactly one message at one revision, unchanged data, on-table-only changes, and no duplicate API events from frontend fan-out. Assert that the API DTO excludes raw frontend-only fields and reload instructions, and that all four accepted collections are present.
  - Re-run `BoaDashboardUpdatePushTests` on the PR branch to protect existing frontend publication behaviour.

- [x] **3. Add the public subscription, access validation, and lifecycle.**
  - Add `JoinAcceptedBoaPush` to `Enact.SignalRNew/Hubs/DataHubNew.cs`, using existing DPS access-context, structured error, usage-reporting, and rate-limiting patterns.
  - Validate the public BOA live-data entitlement and allowance before joining. Add a named DPS subscription entry and use the existing once-per-usage-window recording policy; preserve the existing usage increment rather than inventing a BOA-specific tariff.
  - Extend `DpsSubscriptionHelper` and `DpsReconnectValidationService` so `ReconnectToPush` validates this subscription and its current entitlement. Reconnect restores future delivery and does not replay missed rows or trigger HTTP requests. A rejoin uses the normal validated join method.
  - Integrate the new endpoint with `Services/RateLimiting/`, receiver tracking, and existing API receiver totals where required. Use normal `LeaveGroup` and disconnect cleanup.
  - Protect the reserved API group from authorization bypass through generic `AbstractDataHub.JoinGroup` or alternate join paths. Knowing the group name must not bypass the explicit API checks.
  - Add focused tests alongside `Enact.xUnitTesting/SignalR/Dps/`, `RateLimiting/`, and `ConnectionTracking/`: authorized join, insufficient entitlement, capped allowance, duplicate join accounting, reconnect validation, generic-join bypass rejection, leave, and disconnect. Mock external stores and services.

- [x] **4. Add a dedicated Python accepted-BOA helper.**
  - Add `lcpdelta_python_package/src/lcp_delta/enact/accepted_boa_dps_helper.py`, exposing `AcceptedBoaDPSHelper` and an `AcceptedBoaPush` model via `enact/__init__.py`. The callback receives one structured batch containing date/period/revision and the four public row collections. Preserve the backend batch; do not invoke the callback separately for each row.
  - Provide matching `subscribe_to_accepted_boa_updates()` and `async_subscribe_to_accepted_boa_updates()` methods, unsubscribe, connection status, sync context-manager support, and `close()`/`async_close(wait_for_callbacks=True)`.
  - Reuse authentication, connection supervision, invocation error handling, bounded callback dispatch, and shutdown behaviour from the current multi-series helper through the smallest practical internal extraction/composition. Keep chart-specific parsing, 14-day multi-series lease refresh, and snapshot-gating logic out of the BOA helper. Do not copy the entire multi-series implementation or create a general plugin framework.
  - Preserve public `MultiSeriesDPSHelper` behaviour if internals are extracted. Sharing one physical connection between chart and BOA helpers is not a requirement for this first version.
  - Support synchronous and asynchronous callbacks. Process batch callbacks sequentially by default so source update order is preserved; do not add optional concurrent-callback features unless needed to reuse the existing infrastructure safely. A bounded queue must apply backpressure instead of silently dropping batches.
  - Parse the settlement date as a date, preserve the period/revision, all four collections and each row's `deltaId`, and retain public row values/nulls. Surface structured join errors through `EnactApiError`, and log malformed events or callback failures without leaking credentials or customer payloads.
  - Perform no initial snapshot request, dataframe accumulation, revision-based deduplication, automatic gap recovery, or frontend reload handling. Repeated subscriptions on one helper must not add duplicate handlers/deliveries.

- [x] **5. Verify Python behaviour and cross-language compatibility.**
  - Add credential-free unit coverage in `tests/unit/test_accepted_boa_dps_helper.py`, using fake credentials and SignalR connections in the style of existing helper tests.
  - Cover exact backend fixture decoding; both row sides and changed/removed collections; decimal/null row values; malformed messages; mixed multi-row batches delivered in exactly one callback; empty collections; skipped revisions; source revision resets; and absence of any automatic HTTP fetch or reload behaviour.
  - Cover sync/async subscription parity, structured server errors, repeated subscription, reconnect/rejoin with refreshed authentication, callback ordering/backpressure, callback exceptions, unsubscribe, and shutdown with queued callbacks. Verify reconnect does not silently claim replay or exactly-once delivery.
  - Run existing multi-series and legacy helper tests if shared internals change, then the credential-free unit suite: `python -m pytest lcpdelta_python_package/tests/unit`.
  - Add an integration test under `tests/integration/` for the new public hub contract with bounded waits and unconditional cleanup. Normal subscription success can be checked without assuming a row will change immediately; live event receipt needs a controlled acceptance-update scenario with independently known expected data.
  - Run live integration tests only when explicitly needed and appropriate credentials/environment are available. Leave assertions intact if entitlement, activity, allowance, or network prevents validation; record the limitation. Leave `ENACT_ENV` unset unless staging is explicitly requested.

- [x] **6. Document, validate, and prepare the coordinated release.**
  - Add an environment-variable-based example under `examples/` showing subscription, handling changed/removed collections as one batch, and clean shutdown. Update `README.md` and prepare public API documentation covering the hub join, event schema, live entitlement, subscription accounting, and language-independent use.
  - Explain `deltaId` changes, composite record identity, one source revision per batch, skipped/reset source revisions, applying removals before changed rows within a batch, and possible missed/duplicate events around connection changes. Do not describe this feed as a full state replica.
  - Document that HTTP BM methods remain separately callable by consumers; this release does not add a revision-aware snapshot or automatic recovery.
  - Run backend builds and focused tests for changed projects, following the backend guide. At minimum build `Enact.SignalRNew/Enact.SignalR.csproj` and run the affected xUnit BOA/DPS/dispatch tests. Build the actual engine/buffer consumer if its publication code changes.
  - Run package formatting/lint through pre-commit when available. Record unrelated pre-existing failures and preserve user changes.
  - Document that the compatible backend must deploy before the Python client is released. Record the dependency on PR 80581, update the package version using repository release conventions, and prepare reviewable changes in the two repositories. Actual deployment, package publication, external documentation publication, and PR writes remain separate actions outside this implementation.

### Verification outcome

- Python: **88 credential-free unit tests passed** against the local checkout, including lifecycle message sequencing, heartbeat timeout/recovery, the new helper, existing live helpers, C# contract fixture decoding, backpressure/unsubscribe regression, shutdown timeout cleanup, and existing skip-rates tests.
- Backend: **76 focused xUnit tests passed**, covering accepted-BOA projection/publication, eight actual/predicted-row regression cases, the shared JSON contract fixture, existing frontend BOA behaviour, DPS access/subscriptions, rate limiting, and connection tracking.
- The SignalR project and xUnit project/dependencies (including engine/buffer consumers) built successfully. Existing compiler warnings and unavailable NuGet vulnerability feeds remain; no new compile errors remain.
- Direct Ruff lint and formatting checks passed for all seven changed Python code/test/example files. The configured pre-commit runner could not initialize because the installed `virtualenv` dependency chain lacks `filelock`; no environment repair was attempted.
- Built `dist/lcpdelta-1.1.10-py3-none-any.whl` from final sources. The C# and Python copies of `accepted-boa-push.json` are identical.
- Final diff review completed; Python whitespace checks passed, and backend whitespace checks passed with CRLF recognized as Windows line endings. Existing user endpoint edits and untracked instruction/worktree files were preserved.
- Live integration was **not run**: these additive hub/engine changes are not deployed. The integration subscription test is ready for an entitled account against a compatible deployment; controlled acceptance-update verification remains a release follow-up. Implementation was subsequently committed and published in the PRs recorded below; no deployment or package publication was performed.

## Current Unknowns

- [ ] Release follow-up: live end-to-end validation needs a deployed compatible backend and a controlled acceptance update. The new subscription integration test is implemented but has not been run against production or staging.
- [ ] Release follow-up: public documentation publication remains external to these repositories; a complete repository contract draft is prepared at `docs/accepted-boa-push.md`, with backend deployment notes in `Enact/.worktrees/accepted-boa-api-push/docs/accepted-boa-api-push.md`.

## Decisions

| Decision | Rationale |
|---|---|
| One subscription receives all live GB accepted BOA changes. | User confirmed this scope; consumers do not manage a subscription per settlement period. |
| One batch per source update, with date, period, source revision, and four changed/removed accepted-row collections. | User chose to retain the frontend's batching. Keeps related changes together, reduces messages, and requires less transformation. No per-row operation field is needed. |
| Reuse dashboard source revisions, with no consecutive-delivery guarantee. | User confirmed this choice; avoids a second sequence and its persistence/restart semantics. Each batch carries its source update's revision, and on-table-only updates can create gaps. |
| Reuse existing deltaId verbatim. | Matches the existing change detector. Price/flag changes are delete-old plus upsert-new. |
| Reuse accepted-row comparisons on filtered snapshots and project beside existing publication. | Excludes predictions while correctly handling actual/predicted transitions, without duplicating the comparison algorithm or adding another SignalR listener. |
| Publish a distinct public contract and protect its subscription group. | External clients receive supported public fields and API authorization, independent of frontend payload evolution. |
| No frontend reload field, recovery notification, initial snapshot, automatic fetching, or replay in this version. | The agreed scope is an accepted-row change feed; frontend reload behaviour does not belong in it. |
| Dedicated AcceptedBoaDPSHelper, reusing only necessary internal transport/lifecycle behaviour. | Chart SeriesPing parsing and multi-series joins do not match row events. Existing public helpers remain compatible. |
| One sequential Python callback per batch and no revision-only deduplication. | Preserves source update boundaries/order without treating resettable source revisions as durable delivery IDs. |
| Follow existing public BOA live access and DPS accounting conventions. | Avoids introducing a separate entitlement or pricing policy through an SDK feature. |
| Preserve existing REST DataFrame/container contracts. | This is an additive push API; existing BM fetches do not become a revision-aware recovery API. |
| Backend implementation lives in `Enact/.worktrees/accepted-boa-api-push`, branch `codex/accepted-boa-api-push`, based on PR source commit `28a8ddc9927`. Python uses the same branch name in its own repository. | PR 80581 remains active; isolated backend work preserves other checkouts and user changes. |
| Public rows reuse `BidOfferItemWithAcceptedTimes`, adding only the existing source `deltaId`; the feed excludes both prediction flags before comparison. | Keeps existing public fields and nullable `acceptedTime`, while publishing actual accepted rows and correct additions/removals when prediction status changes. |
| Group and event are both `acceptedBoaUpdated`; join is `JoinAcceptedBoaPush()`. | Fits the existing push-name response and allows non-Python clients to consume the same contract. |
| BOA subscriptions use existing `TrackJoinEnactPush` accounting and a dedicated rate-limit bucket. | Reuses existing API receiver totals and daily DPS subscription recording without a new metrics framework. |
| Existing dispatch provides synchronous enqueue and one sequential partition per group/event; keep that path. | Publication holds the existing period lock through enqueue; no additional ordered dispatcher is needed. Existing overflow/drop and retry semantics still apply. |
| Extract shared private connection/auth/invocation methods into `_signalr_connection.py`; retain separate subscription and callback logic. | Shares transport code without imposing chart parsing, multi-series leases, or snapshot gating on the BOA feed. |
| Pytest explicitly adds the repository `src` directory to its import path. | The initial environment imported an installed package; tests must verify the checkout being changed. |
| Monitor server-wide `heartbeat` receipt in the accepted-BOA helper; ignore `zeroPnUpdate`. | The engine sends heartbeats every five seconds. Use local monotonic time, a configurable 30-second default timeout, one warning per overdue episode and one INFO recovery message. Pause/reset for disconnect, unsubscribe and shutdown. This monitors engine-to-client delivery without treating overdue heartbeats as transport disconnection or forcing reconnects. `zeroPnUpdate` refreshes frontend last-off/zero-PN details and is unrelated to accepted BOA batches. Both handlers prevent missing-method noise; other warnings remain visible. |

## Progress Log

| Date | Event |
|---|---|
| 2026-11-06 | Plan created after inspecting the BOA PR and existing Python/SignalR helpers. User confirmed all-live-GB scope and reuse of source revisions without a consecutive-delivery guarantee. No implementation changes made. |
| 2026-11-06 | Updated the contract and tasks to preserve source-update batches following user feedback. Each API push and Python callback contains all changed/removed accepted rows for one date/period/revision. |
| 2026-11-06 | Implemented backend projection/public DTO, guarded join/reconnect, usage/receiver tracking, and rate limiting. SignalR builds successfully. Initial focused run passed 67/68 tests; fixed the new golden fixture's expected default enum label and queued revalidation. |
| 2026-11-06 | Added AcceptedBoaDPSHelper, private shared connection lifecycle, mock-based tests, subscription integration test, runnable example, and contract documentation. Local-source Python suite passes 67 tests; package 1.1.10 wheel builds. Shared connection also retries initial auth failures and cancels residual tasks on shutdown. |
| 2026-11-06 | Pre-commit could not initialize its virtualenv because the installed environment lacks `filelock`. Direct Ruff validation is used and the environment limitation is retained; existing backend builds also emit NuGet vulnerability-feed and pre-existing compiler warnings. |
| 2026-11-06 | Completed local implementation and final review. All 68 Python unit tests and 68 focused backend tests pass, the shared JSON fixture matches both implementations, direct Ruff checks pass, and the final 1.1.10 wheel builds. Live integration, deployment and external publication remain release follow-ups. |
| 2026-11-06 | Committed and pushed both implementations at the user's request. Backend PR [83869](https://dev.azure.com/lcpdevelop/Enact/_git/Enact/pullrequest/83869) and Python PR [88](https://github.com/lcp-llp/lcpdelta-enact/pull/88) both target main. Backend commit `f1ca294804` depends on PR 80581. Python implementation commit `961d23f` was followed by merge `559c013` from current main; all 77 Python unit tests pass after that merge. Existing endpoint edits and AGENTS.md remain outside the commits. |
| 2026-11-06 | Retargeted backend PR 83869 to `codex/boa-signalr-snapshot`; Python PR 88 still targets main. At the user's request, fixed prediction filtering: eight regression cases failed against the original code and now pass with predictions excluded from both snapshots before comparison. All 76 focused backend tests pass, including unchanged frontend prediction delivery, real-row corrections and same-ID actual/predicted transitions for both row sides and both prediction flags. The API schema and Python code are unchanged; companion documentation was corrected. |
| 2026-11-06 | A staging listener received accepted BOA callbacks and unrelated server-wide heartbeat/zero-PN events. Added explicit no-op handlers for those two broadcasts in the Python helper, retaining other warnings and the existing BOA-only subscription. Updated the existing handler-registration assertion and documentation; all 77 Python unit tests and focused Ruff lint/format checks pass. |
| 2026-11-06 | Replaced the heartbeat no-op with configurable freshness monitoring at the user's request. Added one overdue warning and one INFO recovery message per episode, local monotonic timing, fresh startup/reconnect grace periods, and cancellation on disconnect/unsubscribe/shutdown. Kept zeroPnUpdate ignored after confirming it refreshes frontend last-off/zero-PN metadata. All 87 credential-free Python unit tests and focused Ruff lint/format checks pass. No backend changes were needed. |
| 2026-11-06 | Confirmed earliestDateInBuffer is a server-wide chart-buffer boundary notification used by frontend live/historical request routing. Added a no-op handler to the BOA helper and extended existing registration/heartbeat tests so it stays quiet without reaching BOA callbacks or resetting heartbeat freshness. |
| 2026-11-06 | Added requested lifecycle visibility for connection open, successful subscription, unexpected disconnect, subscription restoration and shutdown. Success messages follow server confirmation; intentional shutdown does not warn about disconnection. The example now presents a table with full row IDs below it and a UTC local-callback receipt timestamp, with a note to replace presentation with application-specific handling. No reconnect hook, automatic recovery, runtime/configuration additions or concurrency changes were added. All 88 credential-free unit tests and focused Ruff checks pass. |
| 2026-11-06 | Simplified example output at the user's request: removed printed deltaIds and added a Yes/No Flagged column using SO/STOR/CADL, matching the site's flagged filter. Raw batches retain their IDs. |
