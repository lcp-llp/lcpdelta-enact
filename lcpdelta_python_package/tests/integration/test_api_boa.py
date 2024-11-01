import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_bm_data_by_period_async():
    res = await enact_api_helper.get_bm_data_by_period_async(date(2024, 8, 1), 1)

    assert res["acceptedBids"].index[0] == "T_HUMR-1"
    assert res["acceptedBids"].iloc[0]["volume"] == -91.7857142857143
    assert res["acceptedBids"].iloc[0]["bidPrice"] == 61.01

    assert res["acceptedOffers"].index[0] == "T_DINO-1"
    assert res["acceptedOffers"].iloc[0]["volume"] == 20.0
    assert res["acceptedOffers"].iloc[0]["offerPrice"] == 97.00

    assert res["tableBids"].index[0] == "T_CGTHW-1"
    assert res["tableBids"].iloc[0]["volume"] == -1.0
    assert res["tableBids"].iloc[0]["bidPrice"] == -116.00

    assert res["tableOffers"].index[0] == "T_GNAPW-1"
    assert res["tableOffers"].iloc[0]["volume"] == 18.0
    assert res["tableOffers"].iloc[0]["offerPrice"] == 9999.00


def test_get_bm_data_by_period_sync():
    res = enact_api_helper.get_bm_data_by_period(date(2024, 8, 1), 1)

    assert res["acceptedBids"].index[0] == "T_HUMR-1"
    assert res["acceptedBids"].iloc[0]["volume"] == -91.7857142857143
    assert res["acceptedBids"].iloc[0]["bidPrice"] == 61.01

    assert res["acceptedOffers"].index[0] == "T_DINO-1"
    assert res["acceptedOffers"].iloc[0]["volume"] == 20.0
    assert res["acceptedOffers"].iloc[0]["offerPrice"] == 97.00

    assert res["tableBids"].index[0] == "T_CGTHW-1"
    assert res["tableBids"].iloc[0]["volume"] == -1.0
    assert res["tableBids"].iloc[0]["bidPrice"] == -116.00

    assert res["tableOffers"].index[0] == "T_GNAPW-1"
    assert res["tableOffers"].iloc[0]["volume"] == 18.0
    assert res["tableOffers"].iloc[0]["offerPrice"] == 9999.00


@pytest.mark.asyncio
async def test_get_bm_data_by_search_async():
    res = await enact_api_helper.get_bm_data_by_search_async(date(2024, 8, 1), "plant", "CARR")

    assert res.iloc[0]["Day"] == "2024-08-01T00:00:00"
    assert res.iloc[0]["Period"] == 19
    assert res.iloc[0]["BMU"] == "T_CARR-1"
    assert res.iloc[0]["Price (£/MWh)"] == 119.0
    assert res.iloc[0]["Volume (MW)"] == 5.231111111111111


def test_get_bm_data_by_search_sync():
    res = enact_api_helper.get_bm_data_by_search(date(2024, 8, 1), "plant", "CARR")

    assert res.iloc[0]["Day"] == "2024-08-01T00:00:00"
    assert res.iloc[0]["Period"] == 19
    assert res.iloc[0]["BMU"] == "T_CARR-1"
    assert res.iloc[0]["Price (£/MWh)"] == 119.0
    assert res.iloc[0]["Volume (MW)"] == 5.231111111111111
