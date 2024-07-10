import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_bm_data_by_period_async():
    res = await enact_api_helper.get_bm_data_by_period_async(date(2023, 6, 22), 1)
    assert res is not None


def test_get_bm_data_by_period_sync():
    res = enact_api_helper.get_bm_data_by_period(date(2023, 6, 22), 1)
    assert res is not None


@pytest.mark.asyncio
async def test_get_bm_data_by_search_async():
    res = await enact_api_helper.get_bm_data_by_search_async(date(2023, 6, 22), "plant", "CARR")
    assert res is not None


def test_get_bm_data_by_search_sync():
    res = enact_api_helper.get_bm_data_by_search(date(2023, 6, 22), "plant", "CARR")
    assert res is not None
