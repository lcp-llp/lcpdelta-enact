import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_N2EX_buy_sell_curves_async():
    res = await enact_api_helper.get_N2EX_buy_sell_curves_async(date(2024, 8, 1))

    assert res["deliveryDateCET"] == "2024-08-01"
    assert res["orderPositions"][0]["deliveryStart"] == "2024-07-31T22:00:00Z"
    assert res["orderPositions"][0]["demandCurve"][0]["price"] == -500.0
    assert res["orderPositions"][0]["demandCurve"][0]["volume"] == 17267.813258411767


def test_get_N2EX_buy_sell_curves_sync():
    res = enact_api_helper.get_N2EX_buy_sell_curves(date(2024, 8, 1))

    assert res["deliveryDateCET"] == "2024-08-01"
    assert res["orderPositions"][0]["deliveryStart"] == "2024-07-31T22:00:00Z"
    assert res["orderPositions"][0]["demandCurve"][0]["price"] == -500.0
    assert res["orderPositions"][0]["demandCurve"][0]["volume"] == 17267.813258411767
