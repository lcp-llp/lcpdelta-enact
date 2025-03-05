import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(2)


expected_columns = [
    "Day",
    "Total",
    "Fcr",
    "AfrrCapacity",
    "AfrrEnergy",
    "DayAhead",
    "IntradayContinuous"
]


@pytest.mark.asyncio
async def test_get_index():

    response_dataframe = enact_api_helper.get_german_index_data(
        date(2024,1,21),
        date(2024,1,23),
        'DA, IDC, aFRR Energy 1Hr 2 Cycle 50MW',
        granularity="Day",
        normalisation = "EuroPerMw"
    )

    assert [column in response_dataframe.columns for column in expected_columns]


def test_get_leaderboard_data_legacy_sync():
    res = enact_api_helper.get_leaderboard_data_legacy(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
    )

    assert [column in res.columns for column in expected_columns]


@pytest.mark.asyncio
async def test_get_leaderboard_data_async():
    res = await enact_api_helper.get_leaderboard_data_async(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
        include_capacity_market_revenues=False,
        ancillary_profit_aggregation="FrequencyAndReserve",
        group_dx=True,
    )

    assert [column in res.columns for column in expected_columns]
    assert [column in res.columns for column in v2_columns]


def test_get_leaderboard_data_sync():
    res = enact_api_helper.get_leaderboard_data(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
        include_capacity_market_revenues=False,
        ancillary_profit_aggregation="FrequencyAndReserve",
        group_dx=True,
    )

    assert [column in res.columns for column in expected_columns]
    assert [column in res.columns for column in v2_columns]
