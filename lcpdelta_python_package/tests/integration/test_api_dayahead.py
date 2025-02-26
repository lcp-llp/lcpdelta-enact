import pytest
import time
import numpy as np

from dateutil.parser import parse
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

    assert isinstance(parse(res[1].iloc[0]["day"]).date(), date)
    assert isinstance(res[2].iloc[0]["daPriceEpexAverage"], np.float64)
    assert isinstance(res[3].iloc[0]["drlVolume"], np.float64)
    assert isinstance(res[4].iloc[0]["daPriceNordpoolAverage"], np.float64)
    assert isinstance(res[5].iloc[0]["dmhPrice"], np.float64)


def test_get_dayahead_data_sync():
    res = enact_api_helper.get_day_ahead_data(
        date(2025, 1, 1),
        date(2025, 2, 1),
        aggregate=False,
        numberOfSimilarDays=10,
        selectedEfaBlocks=[1, 2, 3, 4, 5, 6],
    )

    assert isinstance(parse(res[1].iloc[0]["day"]).date(), date)
    assert isinstance(res[2].iloc[0]["daPriceEpexAverage"], np.float64)
    assert isinstance(res[3].iloc[0]["drlVolume"], np.float64)
    assert isinstance(res[4].iloc[0]["daPriceNordpoolAverage"], np.float64)
    assert isinstance(res[5].iloc[0]["dmhPrice"], np.float64)
