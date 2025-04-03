import pytest
import time
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
    assert subscription_id in enact_dps_helper.data_by_subscription_id

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
      {"seriesId": "ImbalancePriceRealtime", "countryId": "Belgium"},
    ]

    try:
        enact_dps_helper.subscribe_to_multiple_series_updates(handle_data_method, system_request, parse_datetimes=True)
    except Exception as e:
        pytest.fail(f"Subscription to series updates failed: {e}")

    try:
        enact_dps_helper.terminate_hub_connection()
    except Exception as e:
        pytest.fail(f"Termination of the connection failed: {e}")


def test_subscribe_to_multi_plant_series_updates():
    enact_dps_helper = enact.DPSHelper(enact_username, enact_public_api_key)
    assert enact_dps_helper.hub_connection is not None

    plant_request = [
        {"seriesId": "RollingBidsAccepted", "optionIds": ["T_CRUA-2", "T_CRUA-1"], "countryId": "Gb"},
        {"seriesId": "RollingOffersAccepted", "optionIds": ["CCGT", "Battery", ], "countryId": "Gb"},
    ]

    try:
        enact_dps_helper.subscribe_to_multiple_plant_series_updates(handle_data_method, plant_request, parse_datetimes=True)
    except Exception as e:
        pytest.fail(f"Subscription to series updates failed: {e}")

    try:
        enact_dps_helper.terminate_hub_connection()
    except Exception as e:
        pytest.fail(f"Termination of the connection failed: {e}")