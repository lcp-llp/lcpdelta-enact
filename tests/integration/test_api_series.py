import pytest
from datetime import datetime, date
from lcp_delta import enact
from tests.integration import username, public_api_key

enact_api_helper = enact.APIHelper(username, public_api_key)


@pytest.mark.asyncio
async def test_get_series_data_async():
    res = await enact_api_helper.get_series_data_async(
        "OutturnFuel",
        datetime(2023, 6, 21),
        datetime(2023, 6, 23),
        "Gb",
        option_id=["Coal"],
        time_zone_id="UTC",
        parse_datetimes=True,
    )
    assert res is not None


def test_get_series_data_sync():
    res = enact_api_helper.get_series_data(
        "OutturnFuel",
        datetime(2023, 6, 21),
        datetime(2023, 6, 23),
        "Gb",
        option_id=["Coal"],
        time_zone_id="UTC",
        parse_datetimes=True,
    )
    assert res is not None


@pytest.mark.asyncio
async def test_get_series_data_by_fuel_async():
    res = await enact_api_helper.get_series_by_fuel_async(
        "Mel", date(2023, 6, 21), date(2023, 6, 23), "Gb", option_id="Coal", time_zone_id="UTC", parse_datetimes=True
    )
    assert res is not None


def test_get_series_data_by_fuel_sync():
    res = enact_api_helper.get_series_by_fuel(
        "Mel", date(2023, 6, 21), date(2023, 6, 23), "Gb", option_id="Coal", time_zone_id="UTC", parse_datetimes=True
    )
    assert res is not None


@pytest.mark.asyncio
async def test_get_series_data_by_zone_async():
    res = await enact_api_helper.get_series_by_zone_async(
        "Mel", date(2023, 6, 21), date(2023, 6, 23), "Gb", option_id="Z2", time_zone_id="UTC", parse_datetimes=True
    )
    assert res is not None


def test_get_series_data_by_zone_sync():
    res = enact_api_helper.get_series_by_zone(
        "Mel", date(2023, 6, 21), date(2023, 6, 23), "Gb", option_id="Z2", time_zone_id="UTC", parse_datetimes=True
    )
    assert res is not None


@pytest.mark.asyncio
async def test_get_series_data_by_owner_async():
    res = await enact_api_helper.get_series_by_owner_async(
        "Mel",
        date(2023, 6, 21),
        date(2023, 6, 23),
        "Gb",
        option_id="Adela Energy",
        time_zone_id="UTC",
        parse_datetimes=True,
    )
    assert res is not None


def test_get_series_data_by_owner_sync():
    res = enact_api_helper.get_series_by_owner(
        "Mel",
        date(2023, 6, 21),
        date(2023, 6, 23),
        "Gb",
        option_id="Adela Energy",
        time_zone_id="UTC",
        parse_datetimes=True,
    )
    assert res is not None


@pytest.mark.asyncio
async def test_get_series_data_multi_option_async():
    res = await enact_api_helper.get_series_multi_option_async(
        "OutturnFuel",
        date(2023, 6, 21),
        date(2023, 6, 23),
        "Gb",
        option_id=[],
        time_zone_id="UTC",
        parse_datetimes=True,
    )
    assert res is not None


def test_get_series_data_multi_option_sync():
    res = enact_api_helper.get_series_multi_option(
        "OutturnFuel",
        date(2023, 6, 21),
        date(2023, 6, 23),
        "Gb",
        option_id=[],
        time_zone_id="UTC",
        parse_datetimes=True,
    )
    assert res is not None


@pytest.mark.asyncio
async def test_get_series_info_async():
    res = await enact_api_helper.get_series_info_async("OutturnFuel", "Gb")
    assert res is not None


def test_get_series_info_sync():
    res = enact_api_helper.get_series_info("OutturnFuel", "Gb")
    assert res is not None
