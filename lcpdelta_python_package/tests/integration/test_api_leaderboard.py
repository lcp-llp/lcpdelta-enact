import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(2)


expected_columns = [
    "Plant - Name",
    "Plant - BMU",
    "Plant - Fuel",
    "Plant - Owner",
    "Plant - Zone",
    "Plant - Capacity",
    "Plant - Max PN",
    "Profit Normalised - Net",
    "Profit Normalised - Balancing",
    "Profit Normalised - Wholesale",
    "Profit Normalised - DC",
    "Profit Normalised - DR",
    "Profit Normalised - DM",
    "Profit Normalised - FFR",
    "Profit Normalised - STOR",
    "Profit Normalised - BR",
    "Revenue Normalised - Balancing",
    "Revenue Normalised - Wholesale",
    "BM Volume - Bid",
    "BM Volume - Offer",
    "BM Bid price - Min",
    "BM Bid price - Max",
    "BM Bid price - Average",
    "BM Offer price - Min",
    "BM Offer price - Max",
    "BM Offer price - Average",
    "Wholesale Volume - Total",
]


@pytest.mark.asyncio
async def test_get_leaderboard_data_legacy_async():
    res = await enact_api_helper.get_leaderboard_data_legacy_async(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
    )

    assert [column in res.columns for column in expected_columns]


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
