"""Long-running MultiSeriesDPSHelper client with automatic reconnects.

This example shows the recommended shape for a process that listens for Enact
multi-series DPS updates for a long time and lets the helper handle SignalR
reconnects and group restoration.

Run from the repository root:
    python examples/multi_series_dps_long_running_client.py

Credentials are read from:
    LCPDELTA_ENACT_USERNAME
    LCPDELTA_ENACT_API_KEY

Optional:
    RUN_SECONDS=86400
    LOG_LEVEL=INFO

For maintainers testing against staging, set ENACT_ENV=dev before running.
Clients should usually leave ENACT_ENV unset.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from lcp_delta.enact import MultiSeriesDPSHelper


USERNAME_ENV = "LCPDELTA_ENACT_USERNAME"
API_KEY_ENV = "LCPDELTA_ENACT_API_KEY"
RUN_SECONDS = int(os.getenv("RUN_SECONDS", str(24 * 60 * 60)))


# Change this list to the series your process needs to listen to.
SERIES_REQUESTS: list[dict[str, Any]] = [
    {
        "seriesId": "RealtimeFrequency",
        "countryId": "Gb",
    },
    {
        "seriesId": "Mel",
        "countryId": "Gb",
        "optionIds": [["%ALL%"]],
    },
]


def on_chart_push(df, metadata):
    """Handle one received chart push.

    Replace this function with your own application logic, for example writing
    `df` to a queue, database, cache, or alerting pipeline. Keep this callback
    reasonably quick; for slow work, hand off to another worker.
    """
    print("\n" + "=" * 80)
    print(f"Received at: {datetime.now().isoformat(timespec='seconds')}")
    print(f"Series: {metadata.series_id}")
    print(f"Country: {metadata.country_id}")
    print(f"Option IDs: {metadata.option_ids}")
    print(f"Group: {metadata.group_name}")
    print(f"Push ID: {metadata.push_id}")
    print(f"Rows: {len(df)}")
    print(df.tail(10))


async def refresh_snapshot_after_reconnect(helper):
    """Refresh local state before queued live pushes resume after reconnect.

    The helper first drains callbacks queued before the disconnect. Replace
    this with your own API/cache refresh. If it raises, the helper retries
    until `reconnect_callback_timeout_seconds` elapses, then releases queued
    pushes so live processing can continue.
    """
    print("Connection restored; refresh latest snapshot here before live pushes resume.")


def get_credentials() -> tuple[str, str]:
    """Read credentials from environment variables."""
    username = os.getenv(USERNAME_ENV)
    key = os.getenv(API_KEY_ENV)
    if not username or not key:
        raise RuntimeError(f"Set {USERNAME_ENV} and {API_KEY_ENV} before running this example")
    return username, key


def get_lcpdelta_version() -> str:
    try:
        return version("LCPDelta")
    except PackageNotFoundError:
        return "local source checkout (package metadata not installed)"


async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    username, key = get_credentials()

    print("ENACT_ENV:", os.getenv("ENACT_ENV", "default"))
    print("Installed LCPDelta version:", get_lcpdelta_version())

    helper = MultiSeriesDPSHelper(
        username,
        key,
        callback=on_chart_push,
        reconnect_callback=refresh_snapshot_after_reconnect,
        reconnect_callback_timeout_seconds=120,
        # Change these values if your callback workload needs different tuning.
        # callback_queue_maxsize is a backpressure buffer, so keep it above
        # max_workers and large enough for expected reconnect/push bursts.
        concurrent_callbacks=True,
        max_workers=5,
        callback_queue_maxsize=5000,
        # Leave reconnect=True for long-running processes.
        reconnect=True,
        auto_connect=False,
    )

    try:
        group_name = await helper.async_subscribe_to_chart_pushes(SERIES_REQUESTS)
        print(f"Subscribed to group: {group_name}")
        print(f"Listening for {RUN_SECONDS} seconds... Press Ctrl+C to stop early.")

        await asyncio.sleep(RUN_SECONDS)

    finally:
        print("Closing SignalR connection...")
        await helper.async_close(wait_for_callbacks=True)
        print("Closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted.")
