import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


expected_bsad_columns = ["bsadAssetId", "bsadPartyName", "bsadFuelType"]


@pytest.mark.asyncio
async def test_get_bm_data_by_period_async():
    # request a day and period with BSADs
    res = await enact_api_helper.get_bm_data_by_period_async(date(2024, 11, 8), 15)

    assert res["acceptedBids"].index[0] == "T_SGRWO-2"
    assert res["acceptedBids"].iloc[0]["volume"] == -14.000000
    assert res["acceptedBids"].iloc[0]["bidPrice"] == -15.97

    assert res["acceptedOffers"].index[0] == "T_CRUA-1"
    assert res["acceptedOffers"].iloc[0]["volume"] == 10.000000
    assert res["acceptedOffers"].iloc[0]["offerPrice"] == 140.00
    assert [column in res["acceptedOffers"].columns for column in expected_bsad_columns]

    assert res["tableBids"].index[0] == "T_GNAPW-1"
    assert res["tableBids"].iloc[0]["volume"] == -5.0
    assert res["tableBids"].iloc[0]["bidPrice"] == -92.78

    assert res["tableOffers"].index[0] == "T_GNAPW-1"
    assert res["tableOffers"].iloc[0]["volume"] == 14.666666666666666
    assert res["tableOffers"].iloc[0]["offerPrice"] == 9999.00


def test_get_bm_data_by_period_sync():
    # request a day and period with BSADs
    res = enact_api_helper.get_bm_data_by_period(date(2024, 11, 8), 15)

    assert res["acceptedBids"].index[0] == "T_SGRWO-2"
    assert res["acceptedBids"].iloc[0]["volume"] == -14.000000
    assert res["acceptedBids"].iloc[0]["bidPrice"] == -15.97

    assert res["acceptedOffers"].index[0] == "T_CRUA-1"
    assert res["acceptedOffers"].iloc[0]["volume"] == 10.000000
    assert res["acceptedOffers"].iloc[0]["offerPrice"] == 140.00
    assert [column in res["acceptedOffers"].columns for column in expected_bsad_columns]

    assert res["tableBids"].index[0] == "T_GNAPW-1"
    assert res["tableBids"].iloc[0]["volume"] == -5.0
    assert res["tableBids"].iloc[0]["bidPrice"] == -92.78

    assert res["tableOffers"].index[0] == "T_GNAPW-1"
    assert res["tableOffers"].iloc[0]["volume"] == 14.666666666666666
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
