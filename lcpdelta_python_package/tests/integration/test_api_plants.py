import pytest
import time
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_plants_by_fuel_and_country_async():
    res = await enact_api_helper.get_plants_by_fuel_and_country_async("CCGT", "Gb")
    assert res[0] == "T_BARK-1"


def test_get_plants_by_fuel_and_country_sync():
    res = enact_api_helper.get_plants_by_fuel_and_country("CCGT", "Gb")
    assert res[0] == "T_BARK-1"


@pytest.mark.asyncio
async def test_get_plant_details_by_id_async():
    res = await enact_api_helper.get_plant_details_by_id_async("E_HAWKB-1")
    assert res["data"]["plantName"] == "Hawkers Hill"
    assert res["data"]["owner"] == "TagEnergy"
    assert res["data"]["coordinates"] == [-2.20808700157723, 51.0137984995639]
    assert res["data"]["fuel"] == "Battery"
    assert res["data"]["zone"] == "Z17"


def test_get_plant_details_by_id_sync():
    res = enact_api_helper.get_plant_details_by_id("E_HAWKB-1")
    assert res["data"]["plantName"] == "Hawkers Hill"
    assert res["data"]["owner"] == "TagEnergy"
    assert res["data"]["coordinates"] == [-2.20808700157723, 51.0137984995639]
    assert res["data"]["fuel"] == "Battery"
    assert res["data"]["zone"] == "Z17"
