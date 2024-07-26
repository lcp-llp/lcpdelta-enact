import pytest
import time
from datetime import datetime, timezone as tz
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_history_of_forecast_for_given_date_async():
    res = await enact_api_helper.get_history_of_forecast_for_given_date_async(
        "Tsdf",
        datetime(2024, 6, 1, tzinfo=tz.utc),
        "Gb",
    )
    assert res is not None


def test_get_history_of_forecast_for_given_date_sync():
    res = enact_api_helper.get_history_of_forecast_for_given_date(
        "Tsdf",
        datetime(2024, 6, 1, tzinfo=tz.utc),
        "Gb",
    )
    assert res is not None


@pytest.mark.asyncio
async def test_get_history_of_forecast_for_date_range_async():
    res = await enact_api_helper.get_history_of_forecast_for_date_range_async(
        "Tsdf",
        datetime(2024, 6, 1, tzinfo=tz.utc),
        datetime(2024, 6, 2, tzinfo=tz.utc),
        "Gb",
    )
    assert res is not None


def test_get_history_of_forecast_for_date_range_sync():
    res = enact_api_helper.get_history_of_forecast_for_date_range(
        "Tsdf",
        datetime(2024, 6, 1, tzinfo=tz.utc),
        datetime(2024, 6, 2, tzinfo=tz.utc),
        "Gb",
    )
    assert res is not None


@pytest.mark.asyncio
async def test_get_latest_forecast_generated_at_given_time_async():
    res = await enact_api_helper.get_latest_forecast_generated_at_given_time_async(
        "Tsdf",
        datetime(2024, 3, 2, tzinfo=tz.utc),
        datetime(2024, 3, 5, tzinfo=tz.utc),
        "Gb",
        forecast_as_of=datetime(2024, 3, 1, 18, 30, tzinfo=tz.utc),
    )
    assert res is not None


def test_get_history_of_forecast_for_date_range_sync():
    res = enact_api_helper.get_latest_forecast_generated_at_given_time(
        "Tsdf",
        datetime(2024, 3, 2, tzinfo=tz.utc),
        datetime(2024, 3, 5, tzinfo=tz.utc),
        "Gb",
        forecast_as_of=datetime(2024, 3, 1, 18, 30, tzinfo=tz.utc),
    )
    assert res is not None
