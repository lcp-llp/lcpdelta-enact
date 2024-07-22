import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_dayahead_data_async():
    res = await enact_api_helper.get_day_ahead_data_async(
        date(2023, 6, 1),
        date(2023, 6, 20),
        aggregate=False,
        numberOfSimilarDays=10,
        selectedEfaBlocks=[1, 2, 3, 4, 5, 6],
    )
    assert res is not None


def test_get_dayahead_data_sync():
    res = enact_api_helper.get_day_ahead_data(
        date(2023, 6, 1),
        date(2023, 6, 20),
        aggregate=False,
        numberOfSimilarDays=10,
        selectedEfaBlocks=[1, 2, 3, 4, 5, 6],
    )
    assert res is not None
