"""Receive accepted GB BOA batches. Requires live GB BOA API entitlement."""

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
    stream = AcceptedBoaDPSHelper(
        os.environ["LCPDELTA_ENACT_USERNAME"],
        os.environ["LCPDELTA_ENACT_API_KEY"],
        callback=on_update,
        auto_connect=False,
        callback_queue_maxsize=1000,
        heartbeat_timeout_seconds=30,
    )
    try:
        logging.getLogger(__name__).info("Connecting and subscribing to accepted BOA updates...")
        await stream.async_subscribe_to_accepted_boa_updates()
        await asyncio.Event().wait()
    finally:
        await stream.async_close(wait_for_callbacks=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
