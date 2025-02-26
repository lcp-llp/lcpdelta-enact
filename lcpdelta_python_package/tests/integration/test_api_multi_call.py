import pytest
import time
from datetime import date, datetime, timezone as tz
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(2)


def get_series_data():
    return enact_api_helper.get_series_data(
        "DayAheadPrices", date(2025, 1, 1), date(2025, 2, 1), country_id="Gb", time_zone_id="UTC"
    )


async def get_series_data_async():
    return await enact_api_helper.get_series_data_async(
        "DayAheadPrices", date(2025, 1, 1), date(2025, 2, 1), country_id="Gb", time_zone_id="UTC"
    )


def validate_series_response(res):
    assert res.index.name == "GMT Time"
    assert res.index[0] == "2025-01-01T00:00:00Z"
    assert res.columns[0] == "Gb&DayAheadPrices"
    assert isinstance(res.iloc[0, 0], float)


@pytest.mark.asyncio
async def test_looped_series_calls_async():
    for _ in range(6):
        res = await get_series_data_async()
        validate_series_response(res)


def test_looped_series_calls():
    for _ in range(6):
        res = get_series_data()
        validate_series_response(res)


@pytest.mark.asyncio
async def test_multiple_calls_async():
    responses = []
    res_bm = await enact_api_helper.get_bm_data_by_period_async(date(2024, 8, 1), 1)
    responses.append(res_bm)

    time.sleep(1)
    res_lb = await enact_api_helper.get_leaderboard_data_async(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
    )
    responses.append(res_lb)

    time.sleep(1)
    res_an = await enact_api_helper.get_DCH_contracts_async(date(2024, 8, 1))
    responses.append(res_an)

    time.sleep(1)
    res_hf = await enact_api_helper.get_latest_forecast_generated_at_given_time_async(
        "Tsdf",
        datetime(2024, 8, 2, tzinfo=tz.utc),
        datetime(2024, 8, 5, tzinfo=tz.utc),
        "Gb",
        forecast_as_of=datetime(2024, 8, 1, 18, 30, tzinfo=tz.utc),
    )
    responses.append(res_hf)

    time.sleep(1)
    res_n2 = await enact_api_helper.get_N2EX_buy_sell_curves_async(date(2024, 8, 1))
    responses.append(res_n2)

    assert [res is not None for res in responses]


def test_multiple_calls():
    responses = []
    res_bm = enact_api_helper.get_bm_data_by_period(date(2024, 8, 1), 1)
    responses.append(res_bm)

    time.sleep(1)
    res_lb = enact_api_helper.get_leaderboard_data(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
    )
    responses.append(res_lb)

    time.sleep(1)
    res_an = enact_api_helper.get_DCH_contracts(date(2024, 8, 1))
    responses.append(res_an)

    time.sleep(1)
    res_hf = enact_api_helper.get_latest_forecast_generated_at_given_time(
        "Tsdf",
        datetime(2024, 8, 2, tzinfo=tz.utc),
        datetime(2024, 8, 5, tzinfo=tz.utc),
        "Gb",
        forecast_as_of=datetime(2024, 8, 1, 18, 30, tzinfo=tz.utc),
    )
    responses.append(res_hf)

    time.sleep(1)
    res_n2 = enact_api_helper.get_N2EX_buy_sell_curves(date(2024, 8, 1))
    responses.append(res_n2)

    assert [res is not None for res in responses]
