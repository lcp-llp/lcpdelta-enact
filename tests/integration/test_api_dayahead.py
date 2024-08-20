import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_dayahead_data_async():
    res = await enact_api_helper.get_day_ahead_data_async(
        date(2024, 8, 1),
        date(2024, 8, 10),
        aggregate=False,
        numberOfSimilarDays=10,
        selectedEfaBlocks=[1, 2, 3, 4, 5, 6],
    )

    assert res[1].iloc[0]["day"] == "2024-08-09T00:00:00"
    assert res[2].iloc[0]["daPriceEpexAverage"] == -3.68
    assert res[3].iloc[0]["drlVolume"] == 330.0
    assert res[4].iloc[0]["daPriceNordpoolAverage"] == -5.06
    assert res[5].iloc[0]["dmhPrice"] == 6.3


def test_get_dayahead_data_sync():
    res = enact_api_helper.get_day_ahead_data(
        date(2024, 8, 1),
        date(2024, 8, 10),
        aggregate=False,
        numberOfSimilarDays=10,
        selectedEfaBlocks=[1, 2, 3, 4, 5, 6],
    )

    assert res[1].iloc[0]["day"] == "2024-08-09T00:00:00"
    assert res[2].iloc[0]["daPriceEpexAverage"] == -3.68
    assert res[3].iloc[0]["drlVolume"] == 330.0
    assert res[4].iloc[0]["daPriceNordpoolAverage"] == -5.06
    assert res[5].iloc[0]["dmhPrice"] == 6.3
