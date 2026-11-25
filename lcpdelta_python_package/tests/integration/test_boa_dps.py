"""Requires the new backend hub and an account with live GB BOA API access."""

import pytest

from lcp_delta.enact import BoaDPSHelper
from tests.integration import enact_username, enact_public_api_key


@pytest.mark.asyncio
async def test_boa_subscription_and_unsubscribe():
    helper = BoaDPSHelper(enact_username, enact_public_api_key, lambda _batch: None, auto_connect=False)
    try:
        group = await helper.async_subscribe_to_boa_updates(timeout=30)
        assert group == "boaDashboardUpdated"
        assert helper.is_connected
        assert helper.group_names == (group,)
        assert await helper.async_unsubscribe_from_boa_updates(timeout=10)
        assert helper.group_names == ()
    finally:
        await helper.async_close(wait_for_callbacks=True)
