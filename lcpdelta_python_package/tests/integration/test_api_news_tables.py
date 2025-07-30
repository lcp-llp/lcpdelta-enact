import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_news_table_async():
    res = await enact_api_helper.get_news_table_async("Bsad")

    expected_columns = ["Settlement date", "Settlement period", "ID", "Cost", "Volume", "Price", "SO flag", "Reason"]
    assert all(column in res.columns for column in expected_columns)


def test_get_news_table_sync():
    time.sleep(2)
    res = enact_api_helper.get_news_table("Bsad")

    expected_columns = ["Settlement date", "Settlement period", "ID", "Cost", "Volume", "Price", "SO flag", "Reason"]
    assert all(column in res.columns for column in expected_columns)
