import pytest
import time
from datetime import date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_series_data_async():
    res = await enact_api_helper.get_series_data_async(
        "OutturnFuel",
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_id=["Coal"],
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns) == ["Gb&OutturnFuel&Coal"]
    assert res.iloc[0, 0] == 0.0


def test_get_series_data_sync():
    res = enact_api_helper.get_series_data(
        "OutturnFuel",
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_id=["Coal"],
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns) == ["Gb&OutturnFuel&Coal"]
    assert res.iloc[0, 0] == 0.0


@pytest.mark.asyncio
async def test_get_series_data_by_fuel_async():
    res = await enact_api_helper.get_series_by_fuel_async(
        "Mel", date(2024, 8, 1), date(2024, 8, 3), "Gb", option_id="Coal", time_zone_id="UTC", parse_datetimes=True
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns)[0] == "Gb&Mel&T_RATS-3"
    assert res.iloc[0, 0] == 370.0


def test_get_series_data_by_fuel_sync():
    res = enact_api_helper.get_series_by_fuel(
        "Mel", date(2024, 8, 1), date(2024, 8, 3), "Gb", option_id="Coal", time_zone_id="UTC", parse_datetimes=True
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns)[0] == "Gb&Mel&T_RATS-3"
    assert res.iloc[0, 0] == 370.0


@pytest.mark.asyncio
async def test_get_series_data_by_zone_async():
    res = await enact_api_helper.get_series_by_zone_async(
        "Mel", date(2024, 8, 1), date(2024, 8, 3), "Gb", option_id="Z2", time_zone_id="UTC", parse_datetimes=True
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns)[0] == "Gb&Mel&T_MOWEO-3"
    assert res.iloc[0, 0] == 295.0


def test_get_series_data_by_zone_sync():
    res = enact_api_helper.get_series_by_zone(
        "Mel", date(2024, 8, 1), date(2024, 8, 3), "Gb", option_id="Z2", time_zone_id="UTC", parse_datetimes=True
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert "Gb&Mel&T_MOWEO-3" in res.columns
    assert res["Gb&Mel&T_MOWEO-3"].iloc[0] == 295.0


@pytest.mark.asyncio
async def test_get_series_data_by_owner_async():
    res = await enact_api_helper.get_series_by_owner_async(
        "Mel",
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_id="Adela Energy",
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns)[0] == "Gb&Mel&V__JADEL001"
    assert res.iloc[0, 0] == 0.0


def test_get_series_data_by_owner_sync():
    res = enact_api_helper.get_series_by_owner(
        "Mel",
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_id="Adela Energy",
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert "Gb&Mel&V__JADEL001" in res.columns
    assert res["Gb&Mel&V__JADEL001"].iloc[0] == 0.0


@pytest.mark.asyncio
async def test_get_series_data_multi_option_async():
    res = await enact_api_helper.get_series_multi_option_async(
        "OutturnFuel",
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_id=[],
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns)[0] == "Gb&OutturnFuel&Biomass"
    assert res.iloc[0, 0] == 2572.0


def test_get_series_data_multi_option_sync():
    res = enact_api_helper.get_series_multi_option(
        "OutturnFuel",
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_id=[],
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns)[0] == "Gb&OutturnFuel&Biomass"
    assert res.iloc[0, 0] == 2572.0


@pytest.mark.asyncio
async def test_get_series_info_async():
    res = await enact_api_helper.get_series_info_async("OutturnFuel", "Gb")

    assert res["data"]["name"] == "Generation - Fuel"
    assert any(option["id"] == "Biomass" for option in res["data"]["options"][0])
    assert res["data"]["suffix"] == "MW"
    assert any(country["id"] == "Gb" for country in res["data"]["countries"])


def test_get_series_info_sync():
    res = enact_api_helper.get_series_info("OutturnFuel", "Gb")

    assert res["data"]["name"] == "Generation - Fuel"
    assert any(option["id"] == "Biomass" for option in res["data"]["options"][0])
    assert res["data"]["suffix"] == "MW"
    assert any(country["id"] == "Gb" for country in res["data"]["countries"])


@pytest.mark.asyncio
async def test_get_series_multi_series_data_async():
    res = await enact_api_helper.get_multi_series_data_async(
        ["OutturnFuel", "Tsdf", "WindForecast"],
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_ids=["Wind"],
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns) == ["Gb&OutturnFuel&Wind", "Gb&Tsdf", "Gb&WindForecast"]
    assert isinstance(res.iloc[0, 0], float)


def test_get_series_multi_series_data():
    res = enact_api_helper.get_multi_series_data(
        ["OutturnFuel", "Tsdf", "WindForecast"],
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        option_ids=["Wind"],
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    assert list(res.columns) == ["Gb&OutturnFuel&Wind", "Gb&Tsdf", "Gb&WindForecast"]
    assert isinstance(res.iloc[0, 0], float)


@pytest.mark.asyncio
async def test_get_series_data_multi_option_async():
    res = await enact_api_helper.get_multi_plant_series_data_async(
        ["BalancingProfit", "DcEfaRevenue", "WholesaleRevenue"],
        ["CCGT", "Z5", "Flexitricity"],
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    expected_columns = ["Gb&BalancingProfit&T_ROCK-1", "Gb&DcEfaRevenue&E_JAMBB-1", "Gb&WholesaleRevenue&2__AFLEX004"]
    assert [column in list(res.columns) for column in expected_columns]
    assert isinstance(res.iloc[0, 0], float)


def test_get_series_data_multi_option_sync():
    res = enact_api_helper.get_multi_plant_series_data(
        ["BalancingProfit", "DcEfaRevenue", "WholesaleRevenue"],
        ["CCGT", "Z5", "Flexitricity"],
        date(2024, 8, 1),
        date(2024, 8, 3),
        "Gb",
        time_zone_id="UTC",
        parse_datetimes=True,
    )

    assert res.index.name == "GMT Time"
    assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
    expected_columns = ["Gb&BalancingProfit&T_ROCK-1", "Gb&DcEfaRevenue&E_JAMBB-1", "Gb&WholesaleRevenue&2__AFLEX004"]
    assert [column in list(res.columns) for column in expected_columns]
    assert isinstance(res.iloc[0, 0], float)
