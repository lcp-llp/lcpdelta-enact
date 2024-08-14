import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_N2EX_buy_sell_curves_async():
    res = await enact_api_helper.get_N2EX_buy_sell_curves_async(date(2024, 6, 21))
    assert res is not None


def test_get_N2EX_buy_sell_curves_sync():
    res = enact_api_helper.get_N2EX_buy_sell_curves(date(2024, 6, 21))
    assert res is not None
