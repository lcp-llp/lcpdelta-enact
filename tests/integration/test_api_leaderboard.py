import pytest
import time
from datetime import date
from httpx import HTTPStatusError
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_leaderboard_data_async():
    try:
        res = await enact_api_helper.get_leaderboard_data_async(
            date(2023, 6, 21),
            date(2023, 6, 23),
            "Plant",
            "PoundPerMwPerH",
            "WeightedAverageDayAheadPrice",
            "DayAheadForward",
        )
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; happens intermittently on leaderboard endpoint
        else:
            raise


def test_get_leaderboard_data_sync():
    try:
        res = enact_api_helper.get_leaderboard_data(
            date(2023, 6, 21),
            date(2023, 6, 23),
            "Plant",
            "PoundPerMwPerH",
            "WeightedAverageDayAheadPrice",
            "DayAheadForward",
        )
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; happens intermittently on leaderboard endpoint
        else:
            raise
