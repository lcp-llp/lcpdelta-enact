import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_news_table_async():
    res = await enact_api_helper.get_news_table_async("LCP")
    assert res is not None


def test_get_news_table_sync():
    res = enact_api_helper.get_news_table("LCP")
    assert res is not None
