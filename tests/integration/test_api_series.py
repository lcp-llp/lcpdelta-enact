import pytest
import time
from datetime import datetime, date
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


# @pytest.mark.asyncio
# async def test_get_series_data_async():
#     res = await enact_api_helper.get_series_data_async(
#         "OutturnFuel",
#         date(2024, 8, 1),
# 		date(2024, 8, 3),
#         "Gb",
#         option_id=["Coal"],
#         time_zone_id="UTC",
#         parse_datetimes=True,
#     )

#     assert res.index.name == "GMT Time"
#     assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
#     assert list(res.columns) == ["Gb&OutturnFuel&Coal"]
#     assert res.iloc[0, 0] == 0.0


# def test_get_series_data_sync():
#     res = enact_api_helper.get_series_data(
#         "OutturnFuel",
#         date(2024, 8, 1),
# 		date(2024, 8, 3),
#         "Gb",
#         option_id=["Coal"],
#         time_zone_id="UTC",
#         parse_datetimes=True,
#     )

#     assert res.index.name == "GMT Time"
#     assert res.index[0].isoformat() == "2024-07-31T23:00:00+00:00"
#     assert list(res.columns) == ["Gb&OutturnFuel&Coal"]
#     assert res.iloc[0, 0] == 0.0


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


# @pytest.mark.asyncio
# async def test_get_series_data_by_zone_async():
#     res = await enact_api_helper.get_series_by_zone_async(
#         "Mel", date(2023, 6, 21), date(2023, 6, 23), "Gb", option_id="Z2", time_zone_id="UTC", parse_datetimes=True
#     )
#     assert res is not None


# def test_get_series_data_by_zone_sync():
#     res = enact_api_helper.get_series_by_zone(
#         "Mel", date(2023, 6, 21), date(2023, 6, 23), "Gb", option_id="Z2", time_zone_id="UTC", parse_datetimes=True
#     )
#     assert res is not None


# @pytest.mark.asyncio
# async def test_get_series_data_by_owner_async():
#     res = await enact_api_helper.get_series_by_owner_async(
#         "Mel",
#         date(2023, 6, 21),
#         date(2023, 6, 23),
#         "Gb",
#         option_id="Adela Energy",
#         time_zone_id="UTC",
#         parse_datetimes=True,
#     )
#     assert res is not None


# def test_get_series_data_by_owner_sync():
#     res = enact_api_helper.get_series_by_owner(
#         "Mel",
#         date(2023, 6, 21),
#         date(2023, 6, 23),
#         "Gb",
#         option_id="Adela Energy",
#         time_zone_id="UTC",
#         parse_datetimes=True,
#     )
#     assert res is not None


# @pytest.mark.asyncio
# async def test_get_series_data_multi_option_async():
#     res = await enact_api_helper.get_series_multi_option_async(
#         "OutturnFuel",
#         date(2023, 6, 21),
#         date(2023, 6, 23),
#         "Gb",
#         option_id=[],
#         time_zone_id="UTC",
#         parse_datetimes=True,
#     )
#     assert res is not None


# def test_get_series_data_multi_option_sync():
#     res = enact_api_helper.get_series_multi_option(
#         "OutturnFuel",
#         date(2023, 6, 21),
#         date(2023, 6, 23),
#         "Gb",
#         option_id=[],
#         time_zone_id="UTC",
#         parse_datetimes=True,
#     )
#     assert res is not None


# @pytest.mark.asyncio
# async def test_get_series_info_async():
#     res = await enact_api_helper.get_series_info_async("OutturnFuel", "Gb")
#     assert res is not None


# def test_get_series_info_sync():
#     res = enact_api_helper.get_series_info("OutturnFuel", "Gb")
#     assert res is not None
