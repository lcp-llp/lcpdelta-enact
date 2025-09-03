import pytest
import time
import asyncio
import pandas as pd
from lcp_delta import enact
from tests.integration import enact_username, enact_public_api_key

handle_data_method = lambda x: print("Data received:", x)


def teardown_function():
    time.sleep(1)


def test_subscribe_to_series_updates():
    enact_dps_helper = enact.DPSHelper(enact_username, enact_public_api_key)
    assert enact_dps_helper.hub_connection is not None

    try:
        enact_dps_helper.subscribe_to_series_updates(handle_data_method, "RealtimeDemand")
    except Exception as e:
        pytest.fail(f"Subscription to series updates failed: {e}")

    subscription_id = enact_dps_helper._get_subscription_id("RealtimeDemand", "Gb", None)
    assert subscription_id in enact_dps_helper.data_by_single_series_subscription_id

    try:
        enact_dps_helper.terminate_hub_connection()
    except Exception as e:
        pytest.fail(f"Termination of the connection failed: {e}")


def test_subscribe_to_epex_trade_updates():
    enact_dps_helper = enact.DPSHelper(enact_username, enact_public_api_key)
    assert enact_dps_helper.hub_connection is not None

    try:
        enact_dps_helper.subscribe_to_epex_trade_updates(handle_data_method)
    except Exception as e:
        pytest.fail(f"Subscription to EPEX trade updates failed: {e}")

    try:
        enact_dps_helper.terminate_hub_connection()
    except Exception as e:
        pytest.fail(f"Termination of the connection failed: {e}")


def test_subscribe_to_notifications():
    enact_dps_helper = enact.DPSHelper(enact_username, enact_public_api_key)
    assert enact_dps_helper.hub_connection is not None

    try:
        enact_dps_helper.subscribe_to_notifications(handle_data_method)
    except Exception as e:
        pytest.fail(f"Subscription to notifications failed: {e}")

    try:
        enact_dps_helper.terminate_hub_connection()
    except Exception as e:
        pytest.fail(f"Termination of the connection failed: {e}")


def test_subscribe_to_multi_series_updates():
    enact_dps_helper = enact.DPSHelper(enact_username, enact_public_api_key)
    assert enact_dps_helper.hub_connection is not None

    system_request = [
      {"seriesId": "AFRRVolumeRealtime", "optionIds": [["Up"], ["Down"]], "countryId": "Belgium"},
    ]

    try:
        enact_dps_helper.subscribe_to_multiple_series_updates(handle_data_method, system_request, parse_datetimes=True)
    except Exception as e:
        pytest.fail(f"Subscription to series updates failed: {e}")

    try:
        enact_dps_helper.terminate_hub_connection()
    except Exception as e:
        pytest.fail(f"Termination of the connection failed: {e}")

@pytest.mark.asyncio
async def test_subscribe_to_multi_series_updates_async_call_back():

    done_event = asyncio.Event()

    async def async_callback(value):
        assert isinstance(value, pd.DataFrame)
        done_event.set()

    enact_dps_helper = enact.DPSHelper(enact_username, enact_public_api_key, async_mode=True, max_workers= 3)
    assert enact_dps_helper.hub_connection is not None

    system_request = [
        {"seriesId": "RealtimeFrequency", "countryId": "Gb"},
        {"seriesId": "RealtimeDemand", "countryId": "Gb"},
        {"seriesId": "RealtimeFlow", "optionIds": [["%ALL%"]], "countryId": "Gb"},
    ]

    try:
        enact_dps_helper.subscribe_to_multiple_series_updates(async_callback, system_request, parse_datetimes=True)
    except Exception as e:
        pytest.fail(f"Subscription to series updates failed: {e}")

    try:
        await asyncio.wait_for(done_event.wait(), timeout=5*60)
    except asyncio.TimeoutError:
        pytest.fail("Timed out waiting for async_callback to complete")

    try:
        enact_dps_helper.terminate_hub_connection()
    except Exception as e:
        pytest.fail(f"Termination of the connection failed: {e}")