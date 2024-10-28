import pytest
import time
import pandas as pd
from datetime import date, timedelta
from lcp_delta.common.http.exceptions import EnactApiError
from tests.integration import enact_api_helper

yesterday = date.today() - timedelta(days=1)


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_epex_contracts_async():
    res = await enact_api_helper.get_epex_contracts_async(yesterday)

    assert res.index.name == "contractId"
    assert all(column in res.columns for column in ["type", "period"])


def test_get_epex_contracts_sync():
    res = enact_api_helper.get_epex_contracts(yesterday)

    assert res.index.name == "contractId"
    assert all(column in res.columns for column in ["type", "period"])


@pytest.mark.asyncio
async def test_get_epex_order_book_by_contract_id_async():
    try:
        res = await enact_api_helper.get_epex_order_book_by_contract_id_async(13734400)
        assert res is not None
    except EnactApiError as e:
        if "UnknownData: Contract ID Not Found." in str(e):
            pass  # accept this as a pass; cannot guarantee data for a given contract ID
        else:
            raise


def test_get_epex_order_book_by_contract_id_sync():
    try:
        res = enact_api_helper.get_epex_order_book_by_contract_id(13734400)
        assert res is not None
    except EnactApiError as e:
        if "UnknownData: Contract ID Not Found." in str(e):
            pass  # accept this as a pass; cannot guarantee data for a given contract ID
        else:
            raise


@pytest.mark.asyncio
async def test_get_epex_order_book_async():
    res = await enact_api_helper.get_epex_order_book_async("HH", yesterday, 10)

    assert type(res["bidTable"]) == pd.DataFrame
    assert type(res["askTable"]) == pd.DataFrame


def test_get_epex_order_book_sync():
    res = enact_api_helper.get_epex_order_book("HH", yesterday, 10)

    assert type(res["bidTable"]) == pd.DataFrame
    assert type(res["askTable"]) == pd.DataFrame


@pytest.mark.asyncio
async def test_get_epex_trades_by_contract_id_async():
    try:
        res = await enact_api_helper.get_epex_trades_by_contract_id_async(13734400)
        assert res is not None
    except EnactApiError as e:
        if "UnknownData: Contract ID Not Found." in str(e):
            pass  # accept this as a pass; cannot guarantee data for a given contract ID
        else:
            raise


def test_get_epex_trades_by_contract_id_sync():
    try:
        res = enact_api_helper.get_epex_trades_by_contract_id(13734400)
        assert res is not None
    except EnactApiError as e:
        if "UnknownData: Contract ID Not Found." in str(e):
            pass  # accept this as a pass; cannot guarantee data for a given contract ID
        else:
            raise


@pytest.mark.asyncio
async def test_get_epex_trades_async():
    res = await enact_api_helper.get_epex_trades_async("HH", yesterday, 10)


def test_get_epex_trades_sync():
    res = enact_api_helper.get_epex_trades("HH", yesterday, 10)

    assert res.index.name == "tradeId"
    assert all(
        column in res.columns
        for column in ["state", "quantity", "price", "executionTimeGmt", "revisionId", "productType"]
    )
