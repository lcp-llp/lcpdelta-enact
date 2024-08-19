import pytest
import time
import numpy as np
from datetime import datetime, timezone as tz
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_history_of_forecast_for_given_date_async():
    res = await enact_api_helper.get_history_of_forecast_for_given_date_async(
        "Tsdf",
        datetime(2024, 8, 1, tzinfo=tz.utc),
        "Gb",
    )

    assert res.index.name == "Time"
    assert res.index[0] == "2024-07-31T23:00:00Z"
    assert res.columns[0] == "2024/07/30 07:45:00"
    assert isinstance(res.iloc[0, 0], np.float64)


def test_get_history_of_forecast_for_given_date_sync():
    res = enact_api_helper.get_history_of_forecast_for_given_date(
        "Tsdf",
        datetime(2024, 8, 1, tzinfo=tz.utc),
        "Gb",
    )

    assert res.index.name == "Time"
    assert res.index[0] == "2024-07-31T23:00:00Z"
    assert res.columns[0] == "2024/07/30 07:45:00"
    assert isinstance(res.iloc[0, 0], np.float64)


@pytest.mark.asyncio
async def test_get_history_of_forecast_for_date_range_async():
    res = await enact_api_helper.get_history_of_forecast_for_date_range_async(
        "Tsdf",
        datetime(2024, 8, 1, tzinfo=tz.utc),
        datetime(2024, 8, 2, tzinfo=tz.utc),
        "Gb",
    )

    assert res["01/08/2024"].index.name == "Time"
    assert res["01/08/2024"].index[0] == "2024-07-31T23:00:00Z"
    assert res["01/08/2024"].columns[0] == "2024/07/30 07:45:00"
    assert isinstance(res.iloc[0, 0], np.float64)


def test_get_history_of_forecast_for_date_range_sync():
    res = enact_api_helper.get_history_of_forecast_for_date_range(
        "Tsdf",
        datetime(2024, 8, 1, tzinfo=tz.utc),
        datetime(2024, 8, 2, tzinfo=tz.utc),
        "Gb",
    )

    assert res["01/08/2024"].index.name == "Time"
    assert res["01/08/2024"].index[0] == "2024-07-31T23:00:00Z"
    assert res["01/08/2024"].columns[0] == "2024/07/30 07:45:00"
    assert isinstance(res.iloc[0, 0], np.float64)


@pytest.mark.asyncio
async def test_get_latest_forecast_generated_at_given_time_async():
    res = await enact_api_helper.get_latest_forecast_generated_at_given_time_async(
        "Tsdf",
        datetime(2024, 8, 2, tzinfo=tz.utc),
        datetime(2024, 8, 5, tzinfo=tz.utc),
        "Gb",
        forecast_as_of=datetime(2024, 8, 1, 18, 30, tzinfo=tz.utc),
    )

    assert res["02/08/2024"].index.name == "Time"
    assert res["02/08/2024"].index[0] == "2024-08-01T23:00:00Z"
    assert res["02/08/2024"].columns[0] == "2024/08/01 17:15:00"
    assert isinstance(res.iloc[0, 0], np.float64)


def test_get_latest_forecast_generated_at_given_time_sync():
    res = enact_api_helper.get_latest_forecast_generated_at_given_time(
        "Tsdf",
        datetime(2024, 8, 2, tzinfo=tz.utc),
        datetime(2024, 8, 5, tzinfo=tz.utc),
        "Gb",
        forecast_as_of=datetime(2024, 8, 1, 18, 30, tzinfo=tz.utc),
    )

    assert res["02/08/2024"].index.name == "Time"
    assert res["02/08/2024"].index[0] == "2024-08-01T23:00:00Z"
    assert res["02/08/2024"].columns[0] == "2024/08/01 17:15:00"
    assert isinstance(res.iloc[0, 0], np.float64)
