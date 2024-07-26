import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_plants_by_fuel_and_country_async():
    res = await enact_api_helper.get_plants_by_fuel_and_country_async("Biomass", "Gb")
    assert res is not None


def test_get_plants_by_fuel_and_country_sync():
    res = enact_api_helper.get_plants_by_fuel_and_country("Biomass", "Gb")
    assert res is not None


@pytest.mark.asyncio
async def test_get_plant_details_by_id_async():
    res = await enact_api_helper.get_plant_details_by_id_async("T_IRNPS-1")
    assert res is not None


def test_get_plant_details_by_id_sync():
    res = enact_api_helper.get_plant_details_by_id("T_IRNPS-1")
    assert res is not None
