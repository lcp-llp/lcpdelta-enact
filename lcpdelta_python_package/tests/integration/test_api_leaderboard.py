import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(2)


v1_columns = [
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


v2_columns = [ # Plant - Co-located fuel, Profit - CM are optional extras, also return is dependant on the ancillary split and dx grouping options chosen. Availability columns always show even though its use in the values is optional.
    'Plant - Name',
    'Plant - Fuel',
    'Plant - Co-located fuel',
    'Plant - Owner',
    'Plant - Time First Active',
    'Plant - Zone',
    'Plant - Capacity',
    'Plant - Demand Capacity',
    'Plant - Max PN',
    'Plant - Max MEL',
    'Plant - Min MIL',
    'Plant - Availability',
    'BESS - BM Status',
    'BESS - Optimiser',
    'BESS - Energy',
    'BESS - Duration',
    'BESS - No. Cycles',
    'Profit - Net',
    'Profit - BM',
    'Profit - Wholesale',
    'Profit - Wholesale Import',
    'Profit - Wholesale Export',
    'Profit - Frequency',
    'Profit - Reserve',
    'Profit - CM',
    'Profit - Non-Delivery Charge',
    'BM Profit Breakdown - Bids',
    'BM Profit Breakdown - Offers',
    'BM Profit Breakdown - System',
    'BM Profit Breakdown - Energy',
    'Revenue - BM',
    'Revenue - Wholesale',
    'BM Revenue Breakdown - Bids',
    'BM Revenue Breakdown - Offers',
    'BM Revenue Breakdown - System',
    'BM Revenue Breakdown - Energy',
    'BM Volume - Bid',
    'BM Volume - Offer',
    'BM Volume - System',
    'BM Volume - Energy',
    'BM Volume - Non-Delivered Bid',
    'BM Volume - Non-Delivered Offer',
    'BM Bid Price - Min',
    'BM Bid Price - Max',
    'BM Bid Price - Average',
    'BM Offer Price - Min',
    'BM Offer Price - Max',
    'BM Offer Price - Average',
    'Wholesale Volume - Total',
    'Wholesale Volume - Import',
    'Wholesale Volume - Export'
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

    assert all(column in res.columns for column in v1_columns), f"Missing columns: {[col for col in v1_columns if col not in res.columns]}"



def test_get_leaderboard_data_legacy_sync():
    res = enact_api_helper.get_leaderboard_data_legacy(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
    )

    assert all(column in res.columns for column in v1_columns), f"Missing columns: {[col for col in v1_columns if col not in res.columns]}"


@pytest.mark.asyncio
async def test_get_leaderboard_data_async():
    res = await enact_api_helper.get_leaderboard_data_async(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
        include_capacity_market_revenues=True,
        ancillary_profit_aggregation="FrequencyAndReserve",
        group_dx=True,
        show_co_located_fuels=True,
        account_for_availability_in_normalisation=True,
    )

    assert all(column in res.columns for column in v2_columns), f"Missing columns: {[col for col in v2_columns if col not in res.columns]}"


def test_get_leaderboard_data_sync():
    res = enact_api_helper.get_leaderboard_data(
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Plant",
        "PoundPerMwPerH",
        "WeightedAverageDayAheadPrice",
        "DayAheadForward",
        include_capacity_market_revenues=True,
        ancillary_profit_aggregation="FrequencyAndReserve",
        group_dx=True,
        show_co_located_fuels=True,
        account_for_availability_in_normalisation=True,
    )

    assert all(column in res.columns for column in v2_columns), f"Missing columns: {[col for col in v2_columns if col not in res.columns]}"
