import pytest
import time
from datetime import date, timedelta
from httpx import HTTPStatusError
from tests.integration import enact_api_helper

yesterday = date.today() - timedelta(days=1)


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_epex_contracts_async():
    res = await enact_api_helper.get_epex_contracts_async(yesterday)
    assert res is not None


def test_get_epex_contracts_sync():
    res = enact_api_helper.get_epex_contracts(yesterday)
    assert res is not None


@pytest.mark.asyncio
async def test_get_epex_order_book_by_contract_id_async():
    try:
        res = await enact_api_helper.get_epex_order_book_by_contract_id_async(13734400)
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; cannot guarantee data for a given contract ID
        else:
            raise


def test_get_epex_order_book_by_contract_id_sync():
    try:
        res = enact_api_helper.get_epex_order_book_by_contract_id(13734400)
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; cannot guarantee data for a given contract ID
        else:
            raise


@pytest.mark.asyncio
async def test_get_epex_order_book_async():
    res = await enact_api_helper.get_epex_order_book_async("HH", yesterday, 10)
    assert res is not None


def test_get_epex_order_book_sync():
    res = enact_api_helper.get_epex_order_book("HH", yesterday, 10)
    assert res is not None


@pytest.mark.asyncio
async def test_get_epex_trades_by_contract_id_async():
    try:
        res = await enact_api_helper.get_epex_trades_by_contract_id_async(13734400)
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; cannot guarantee data for a given contract ID
        else:
            raise


def test_get_epex_trades_by_contract_id_sync():
    try:
        res = enact_api_helper.get_epex_trades_by_contract_id(13734400)
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; cannot guarantee data for a given contract ID
        else:
            raise


@pytest.mark.asyncio
async def test_get_epex_trades_async():
    try:
        res = await enact_api_helper.get_epex_trades_async("HH", yesterday, 10)
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; cannot guarantee data for dynamic date
        else:
            raise


def test_get_epex_trades_sync():
    try:
        res = enact_api_helper.get_epex_trades("HH", yesterday, 10)
        assert res is not None
    except HTTPStatusError as e:
        if "UnknownData" in str(e):
            pass  # accept this error; cannot guarantee data for dynamic date
        else:
            raise
