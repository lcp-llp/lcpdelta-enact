import pytest
import time
from datetime import date
from tests.integration import enact_api_helper

def teardown_function():
    time.sleep(2)

expected_data_columns = [
    "Total",
    "Fcr",
    "AfrrCapacity",
    "AfrrEnergy",
    "DayAhead",
    "IntradayContinuous"
]

def test_get_index_sync():
    response_dataframe = enact_api_helper.get_german_index_data(
        date(2024,1,21),
        date(2024,1,23),
        '107b466d-56b5-41c8-9af5-89b47beb086e',
        granularity="Day",
        normalisation = "EuroPerMw"
    )

    assert all(column in response_dataframe.columns for column in expected_data_columns), f"Missing columns: {[col for col in expected_data_columns if col not in response_dataframe.columns]}"

@pytest.mark.asyncio
async def test_get_index_async():
    response_dataframe = await enact_api_helper.get_german_index_data_async(
        date(2024,1,21),
        date(2024,1,23),
        '107b466d-56b5-41c8-9af5-89b47beb086e',
        granularity="Day",
        normalisation = "EuroPerMw"
    )

    assert all(column in response_dataframe.columns for column in expected_data_columns), f"Missing columns: {[col for col in expected_data_columns if col not in response_dataframe.columns]}"