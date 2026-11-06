"""Listen for accepted GB BOA changes and display each batch as a table.

Requires live GB BOA public API entitlement and a compatible backend. One
subscription covers all live GB accepted bids/offers; there is no period filter.

Setup and run from the repository root (PowerShell):
    python -m pip install -e ".[dev]"
    $env:LCPDELTA_ENACT_USERNAME = "<your username>"
    $env:LCPDELTA_ENACT_API_KEY = "<your public API key>"
    python examples/accepted_boa_client.py

Credentials are read from environment variables, not a .env file. Keep real
credentials out of this file and source control.

For maintainers explicitly testing staging, set $env:ENACT_ENV = "dev" before
running. Clients should normally leave ENACT_ENV unset.

The listener runs until Ctrl+C. It logs subscription/connection lifecycle events
and warns if no engine heartbeat arrives for 30 seconds. Shutdown closes the
connection and waits for queued callbacks, subject to the helper's close timeout.

To use the data in your own application, replace on_update() below. Each callback
receives one AcceptedBoaPush containing:
    date, period, revision: GB settlement date/period and source revision.
    accepted_bids, accepted_offers: complete changed rows to insert or replace.
    removed_accepted_bids, removed_accepted_offers: previous rows to remove.

Rows are dictionaries. The table only displays selected fields; the raw rows
also include deltaId for matching records. If maintaining state, apply removals
before changed rows, keyed by date, period, bid/offer side and deltaId. Keep the
callback quick; hand slow storage or processing work off to another worker.

This feed starts with future changes, without an initial snapshot. Reconnects
restore future delivery; missed changes are not replayed. Source revisions can
skip or reset and cannot be used to prove that every update was received.

See docs/accepted-boa-push.md for the full contract and delivery semantics.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import pandas as pd

from lcp_delta.enact import AcceptedBoaDPSHelper, AcceptedBoaPush


def on_update(batch: AcceptedBoaPush) -> None:
    """Display one batch for testing; replace this with your own raw-data handling.

    The table is only an example presentation. The batch contains the original
    public row dictionaries, ready for your own storage, alerts or processing.
    Keep callbacks quick; hand slow work off to another worker.
    """
    received_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # Each collection contains changed rows only, not a complete snapshot.
    # If maintaining state, remove old deltaIds before inserting changed rows.
    table_rows = []
    for action, side, rows, price_key in (
        ("REMOVED", "BID", batch.removed_accepted_bids, "bidPrice"),
        ("REMOVED", "OFFER", batch.removed_accepted_offers, "offerPrice"),
        ("CHANGED", "BID", batch.accepted_bids, "bidPrice"),
        ("CHANGED", "OFFER", batch.accepted_offers, "offerPrice"),
    ):
        for row in rows:
            values = (
                len(table_rows) + 1,
                action,
                side,
                row.get("plantId"),
                row.get("bodId"),
                row.get("volume"),
                row.get(price_key),
                # Match the site's flagged filter: SO, STOR or CADL.
                "Yes" if any(row.get(flag, False) for flag in ("soFlag", "storFlag", "cadlFlag")) else "No",
                row.get("acceptedTime"),
            )
            table_rows.append(["-" if value is None else str(value) for value in values])

    table = pd.DataFrame(
        table_rows, columns=["Row", "Change", "Side", "Plant", "BOD", "Volume", "Price", "Flagged", "Accepted time"]
    )
    print(
        f"\nReceived at: {received_at} (local callback time)\n"
        f"Settlement date: {batch.date} | Period: {batch.period} | Revision: {batch.revision}\n"
        f"{table.to_string(index=False)}",
        flush=True,
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    username = os.getenv("LCPDELTA_ENACT_USERNAME")
    key = os.getenv("LCPDELTA_ENACT_API_KEY")
    if not username or not key:
        raise RuntimeError("Set LCPDELTA_ENACT_USERNAME and LCPDELTA_ENACT_API_KEY before running this example")

    stream = AcceptedBoaDPSHelper(
        username,
        key,
        # Replace this callback with your own raw-batch handling.
        callback=on_update,
        # Defer connecting until subscribing below; reconnect defaults to True.
        auto_connect=False,
        # Bound queued plus running callbacks; slow consumers apply backpressure.
        callback_queue_maxsize=1000,
        # Warn once when heartbeats are overdue, then log when they resume.
        heartbeat_timeout_seconds=30,
    )
    try:
        logging.getLogger(__name__).info("Connecting and subscribing to accepted BOA updates...")
        await stream.async_subscribe_to_accepted_boa_updates()
        logging.getLogger(__name__).info("Listening for accepted BOA changes. Press Ctrl+C to stop.")
        await asyncio.Event().wait()
    finally:
        # Always close, including if subscribing fails or Ctrl+C cancels main().
        await stream.async_close(wait_for_callbacks=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
