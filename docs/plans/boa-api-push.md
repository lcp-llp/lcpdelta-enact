# Shared BOA API and Python changes

Updated 25 November 2026 following the agreed changes to the original accepted-only design.

## Decisions

- The backend publishes one frontend-compatible `boaDashboardUpdated` delta per settlement period. API and frontend connections consume the same publication.
- `JoinBoaPush()` maintains a rolling 24-hour UTC window from the current settlement period: 48 half-hour groups, end exclusive. Existing GB conversion handles 46/48/50-period days. Frontend period subscriptions remain unchanged.
- Python exposes `BoaDPSHelper`, one subscription and one callback receiving `BoaPush`. Accepted and on-table bids/offers each have changed and removed collections.
- The wire decoder reads the nested `datePeriod`/`item` envelope and projects rows to the established public format. Internal frontend fields are omitted; public Boolean flags, coordinates, BSAD metadata and acceptance timestamps retain their public representation.
- Predicted rows are ignored, including predicted removals. They never synthesize a removal of a previously delivered actual row. There is no local row-history cache.
- `reloadRequired` and `previousRevision` are ignored and omitted from Python callback objects. Empty cleaned batches do not invoke callbacks. No reload warnings, HTTP fetches or revision-gap/deduplication workflow is added.
- Existing callback ordering, bounded backpressure, connection restoration, heartbeat monitoring and shutdown behaviour are retained. Restoring a connection only resumes future delivery.

## Scope and verification

Backend changes belong to Enact PR 83869, based on the shared publisher already merged in PR 80581. Python changes belong to `lcp-llp/lcpdelta-enact` PR 88. This supersedes the earlier `JoinAcceptedBoaPush`/`acceptedBoaUpdated` flat accepted-only protocol and all-period coverage.

Verify backend window endpoints, clock changes, add-before-remove rollover, idempotence, failure cleanup and frontend/API isolation. Verify Python projection, all eight collections, single-callback delivery, ignored predictions/reload flags, reconnect/unsubscribe, queue ordering and unchanged multi-series transport behaviour. Use credential-free unit tests; run live integration only with an explicitly selected compatible deployment and test account.

The current contract and runnable example are [documented here](../boa-push.md). External API-documentation publication is separate from repository changes.
