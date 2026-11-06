"""Receive accepted GB BOA batches. Requires live GB BOA API entitlement."""

import asyncio
import logging
import os

from lcp_delta.enact import AcceptedBoaDPSHelper, AcceptedBoaPush


def on_update(batch: AcceptedBoaPush) -> None:
    # Each collection contains changed rows only, not a complete snapshot.
    # If maintaining state, remove old deltaIds before inserting changed rows.
    print(
        batch.date,
        batch.period,
        batch.revision,
        f"changed bids={len(batch.accepted_bids)}, offers={len(batch.accepted_offers)}",
        f"removed bids={len(batch.removed_accepted_bids)}, offers={len(batch.removed_accepted_offers)}",
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
        await stream.async_subscribe_to_accepted_boa_updates()
        await asyncio.Event().wait()
    finally:
        await stream.async_close(wait_for_callbacks=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
