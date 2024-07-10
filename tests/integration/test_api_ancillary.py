import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_DCH_contracts_async():
    res = await enact_api_helper.get_DCH_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_DCH_contracts_sync():
    res = enact_api_helper.get_DCH_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_DCL_contracts_async():
    res = await enact_api_helper.get_DCL_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_DCL_contracts_sync():
    res = enact_api_helper.get_DCL_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_DMH_contracts_async():
    res = await enact_api_helper.get_DMH_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_DMH_contracts_sync():
    res = enact_api_helper.get_DMH_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_DML_contracts_async():
    res = await enact_api_helper.get_DML_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_DML_contracts_sync():
    res = enact_api_helper.get_DML_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_DRH_contracts_async():
    res = await enact_api_helper.get_DRH_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_DRH_contracts_sync():
    res = enact_api_helper.get_DRH_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_DRL_contracts_async():
    res = await enact_api_helper.get_DRL_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_DRL_contracts_sync():
    res = enact_api_helper.get_DRL_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_DCH_contracts_async():
    res = await enact_api_helper.get_DCH_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_DCH_contracts_sync():
    res = enact_api_helper.get_DCH_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_FFR_contracts_async():
    res = await enact_api_helper.get_FFR_contracts_async(150)
    assert res is not None


def test_get_FFR_contracts_sync():
    res = enact_api_helper.get_FFR_contracts(150)
    assert res is not None


@pytest.mark.asyncio
async def test_get_ancillary_contract_data_async():
    res = await enact_api_helper.get_ancillary_contract_data_async("DynamicContainmentEfa", "12-2023", "5")
    assert res is not None


def test_get_ancillary_contract_data_sync():
    res = enact_api_helper.get_ancillary_contract_data("DynamicContainmentEfa", "12-2023", "5")
    assert res is not None


@pytest.mark.asyncio
async def test_get_MFR_contracts_async():
    res = await enact_api_helper.get_MFR_contracts_async(3, 2024)
    assert res is not None


def test_get_MFR_contracts_sync():
    res = enact_api_helper.get_MFR_contracts(3, 2024)
    assert res is not None


@pytest.mark.asyncio
async def test_get_NBR_contracts_async():
    res = await enact_api_helper.get_NBR_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_NBR_contracts_sync():
    res = enact_api_helper.get_NBR_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_PBR_contracts_async():
    res = await enact_api_helper.get_PBR_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_PBR_contracts_sync():
    res = enact_api_helper.get_PBR_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_SFFR_contracts_async():
    res = await enact_api_helper.get_SFFR_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_SFFR_contracts_sync():
    res = enact_api_helper.get_SFFR_contracts(date(2023, 5, 5))
    assert res is not None


@pytest.mark.asyncio
async def test_get_STOR_contracts_async():
    res = await enact_api_helper.get_STOR_contracts_async(date(2023, 5, 5))
    assert res is not None


def test_get_STOR_contracts_sync():
    res = enact_api_helper.get_STOR_contracts(date(2023, 5, 5))
    assert res is not None
