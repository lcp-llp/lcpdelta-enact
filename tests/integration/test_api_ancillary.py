import pytest
import time
import pandas as pd
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


ancillary_columns = [
    "acceptanceRatio",
    "marketName",
    "biddingLevelName",
    "marketProductId",
    "auctionId",
    "deliveryStartGmt",
    "deliveryEndGmt",
    "efa",
    "orderType",
    "volumeOffered",
    "limitPrice",
    "basketId",
    "loopedBasketId",
    "orderEntryTime",
    "status",
    "accepted",
    "volumeAccepted",
    "availabilityFee",
    "reasonRejected",
    "settlementCurrency",
    "optimiser",
    "enactId",
    "owner",
    "fuel",
]

balancing_reserve_columns = [
    "marketName",
    "marketProductId",
    "auctionId",
    "deliveryStartGmt",
    "deliveryEndGmt",
    "deliveryDatePeriod",
    "orderType",
    "volumeOffered",
    "price",
    "basketId",
    "loopedBasketId",
    "orderEntryTime",
    "status",
    "accepted",
    "acceptanceRatio",
    "volume",
    "availabilityPricePerMwh",
    "reasonRejected",
    "enactId",
    "owner",
    "fuel",
]

sffr_columns = [
    "efaDateOfDelivery",
    "gmtDeliveryStart",
    "gmtDeliveryEnd",
    "efa",
    "service",
    "offeredVolume",
    "offeredPrice",
    "rank",
    "accepted",
    "volumeAccepted",
    "availabilityFee",
    "enactId",
    "owner",
    "fuel",
]

stor_columns = [
    "gmtDeliveryStart",
    "gmtDeliveryEnd",
    "tenderSubmissionId",
    "isBm",
    "tenderedMw",
    "minAcceptableMw",
    "tenderedAvailabilityPrice",
    "tenderRoundId",
    "buyCurveId",
    "clearingVolume",
    "clearingPrice",
    "accepted",
    "enactId",
    "owner",
    "fuel",
]


@pytest.mark.asyncio
async def test_get_DCH_contracts_async():
    res = await enact_api_helper.get_DCH_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111407"
    assert all(column in res.columns for column in ancillary_columns)


def test_get_DCH_contracts_sync():
    res = enact_api_helper.get_DCH_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111407"
    assert all(column in res.columns for column in ancillary_columns)


@pytest.mark.asyncio
async def test_get_DCL_contracts_async():
    res = await enact_api_helper.get_DCL_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111406"
    assert all(column in res.columns for column in ancillary_columns)


def test_get_DCL_contracts_sync():
    res = enact_api_helper.get_DCL_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111406"
    assert all(column in res.columns for column in ancillary_columns)


@pytest.mark.asyncio
async def test_get_DMH_contracts_async():
    res = await enact_api_helper.get_DMH_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6112209"
    assert all(column in res.columns for column in ancillary_columns)


def test_get_DMH_contracts_sync():
    res = enact_api_helper.get_DMH_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6112209"
    assert all(column in res.columns for column in ancillary_columns)


@pytest.mark.asyncio
async def test_get_DML_contracts_async():
    res = await enact_api_helper.get_DML_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111969"
    assert all(column in res.columns for column in ancillary_columns)


def test_get_DML_contracts_sync():
    res = enact_api_helper.get_DML_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111969"
    assert all(column in res.columns for column in ancillary_columns)


@pytest.mark.asyncio
async def test_get_DRH_contracts_async():
    res = await enact_api_helper.get_DRH_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111970"
    assert all(column in res.columns for column in ancillary_columns)


def test_get_DRH_contracts_sync():
    res = enact_api_helper.get_DRH_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111970"
    assert all(column in res.columns for column in ancillary_columns)


@pytest.mark.asyncio
async def test_get_DRL_contracts_async():
    res = await enact_api_helper.get_DRL_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111971"
    assert all(column in res.columns for column in ancillary_columns)


def test_get_DRL_contracts_sync():
    res = enact_api_helper.get_DRL_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111971"
    assert all(column in res.columns for column in ancillary_columns)


@pytest.mark.asyncio
async def test_get_FFR_contracts_async():
    res = await enact_api_helper.get_FFR_contracts_async(150)

    assert res.index.name == "tenderNumber"
    assert res.iloc[0]["availabilityFee"] == 262.17
    assert res.iloc[0]["enactId"] == "2__NFSEN007"


def test_get_FFR_contracts_sync():
    res = enact_api_helper.get_FFR_contracts(150)

    assert res.index.name == "tenderNumber"
    assert res.iloc[0]["availabilityFee"] == 262.17
    assert res.iloc[0]["enactId"] == "2__NFSEN007"


@pytest.mark.asyncio
async def test_get_ancillary_contract_data_async():
    res = await enact_api_helper.get_ancillary_contract_data_async("DynamicContainmentEfa", "8-2024", "1")

    assert res["data"][0]["contractType"] == "DynamicContainmentEfa"
    assert res["data"][0]["plants"][0]["orderId"] == "6111406"


def test_get_ancillary_contract_data_sync():
    res = enact_api_helper.get_ancillary_contract_data("DynamicContainmentEfa", "8-2024", "1")

    assert res["data"][0]["contractType"] == "DynamicContainmentEfa"
    assert res["data"][0]["plants"][0]["orderId"] == "6111406"


@pytest.mark.asyncio
async def test_get_MFR_contracts_async():
    res = await enact_api_helper.get_MFR_contracts_async(3, 2024)
    assert type(res) is pd.DataFrame


def test_get_MFR_contracts_sync():
    res = enact_api_helper.get_MFR_contracts(3, 2024)
    assert type(res) is pd.DataFrame


@pytest.mark.asyncio
async def test_get_NBR_contracts_async():
    res = await enact_api_helper.get_NBR_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111346"
    assert all(column in res.columns for column in balancing_reserve_columns)


def test_get_NBR_contracts_sync():
    res = enact_api_helper.get_NBR_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111346"
    assert all(column in res.columns for column in balancing_reserve_columns)


@pytest.mark.asyncio
async def test_get_PBR_contracts_async():
    res = await enact_api_helper.get_PBR_contracts_async(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111383"
    assert all(column in res.columns for column in balancing_reserve_columns)


def test_get_PBR_contracts_sync():
    res = enact_api_helper.get_PBR_contracts(date(2024, 8, 1))

    assert res.index.name == "orderId"
    assert res.index[0] == "6111383"
    assert all(column in res.columns for column in balancing_reserve_columns)


@pytest.mark.asyncio
async def test_get_SFFR_contracts_async():
    res = await enact_api_helper.get_SFFR_contracts_async(date(2024, 8, 1))

    assert res.iloc[0]["enactId"] == "IPSWA-1"
    assert res.iloc[0]["offeredVolume"] == 20.0
    assert res.iloc[0]["offeredPrice"] == 0.01
    assert all(column in res.columns for column in sffr_columns)


def test_get_SFFR_contracts_sync():
    res = enact_api_helper.get_SFFR_contracts(date(2024, 8, 1))

    assert res.iloc[0]["enactId"] == "IPSWA-1"
    assert res.iloc[0]["offeredVolume"] == 20.0
    assert res.iloc[0]["offeredPrice"] == 0.01
    assert all(column in res.columns for column in sffr_columns)


@pytest.mark.asyncio
async def test_get_STOR_contracts_async():
    res = await enact_api_helper.get_STOR_contracts_async(date(2024, 8, 1))

    assert res.iloc[0]["tenderRoundId"] == "TRN-2949"
    assert res.iloc[0]["clearingVolume"] == 60.0
    assert res.iloc[0]["clearingPrice"] == 0.72
    assert all(column in res.columns for column in stor_columns)


def test_get_STOR_contracts_sync():
    res = enact_api_helper.get_STOR_contracts(date(2024, 8, 1))

    assert res.iloc[0]["tenderRoundId"] == "TRN-2949"
    assert res.iloc[0]["clearingVolume"] == 60.0
    assert res.iloc[0]["clearingPrice"] == 0.72
    assert all(column in res.columns for column in stor_columns)
