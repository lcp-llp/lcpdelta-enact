from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from lcp_delta.enact.api_helper import APIHelper
from lcp_delta.enact.services import skip_rates_service


def test_generate_skip_rates_request_uses_public_api_defaults():
    request = skip_rates_service.generate_request(date(2026, 1, 1), date(2026, 1, 31))

    assert request == {
        "From": "2026-01-01",
        "To": "2026-01-31",
        "SelectedGranularity": "Day",
        "ActionType": "Combined",
        "Stage": "Five",
        "Source": "Psa",
        "SelectedZone": ["All Zones"],
        "SelectedFuel": ["All Fuels"],
        "SelectedOwners": [],
        "SelectedOptimisers": [],
        "SelectedCapacity": None,
        "SelectedDuration": None,
        "SelectedPlants": [],
    }


def test_generate_skip_rates_request_includes_optional_filters():
    request = skip_rates_service.generate_request(
        date(2026, 1, 1),
        date(2026, 1, 31),
        selected_granularity="HalfHourly",
        action_type="Bid",
        stage="Four",
        source="AllBm",
        selected_zone=["Z1", "Z2"],
        selected_fuel=["Battery"],
        selected_owners=["Owner A"],
        selected_optimisers=["Optimiser A"],
        selected_capacity=(10, 100),
        selected_duration=[1.5, 4.0],
        selected_plants=["T_TEST-1"],
    )

    assert request["SelectedGranularity"] == "HalfHourly"
    assert request["ActionType"] == "Bid"
    assert request["Stage"] == "Four"
    assert request["Source"] == "AllBm"
    assert request["SelectedZone"] == ["Z1", "Z2"]
    assert request["SelectedFuel"] == ["Battery"]
    assert request["SelectedOwners"] == ["Owner A"]
    assert request["SelectedOptimisers"] == ["Optimiser A"]
    assert request["SelectedCapacity"] == [10, 100]
    assert request["SelectedDuration"] == [1.5, 4.0]
    assert request["SelectedPlants"] == ["T_TEST-1"]


def test_generate_skip_rates_request_validates_list_filters():
    with pytest.raises(ValueError, match="SelectedFuel input must be a list of strings"):
        skip_rates_service.generate_request(date(2026, 1, 1), date(2026, 1, 31), selected_fuel="Battery")


def test_generate_skip_rates_request_validates_range_filters():
    with pytest.raises(ValueError, match="SelectedCapacity input must contain exactly two values"):
        skip_rates_service.generate_request(date(2026, 1, 1), date(2026, 1, 31), selected_capacity=[10])


def test_process_skip_rates_response_keeps_gb_local_time_as_column():
    response = {
        "data": [
            ["GB Local Time", "Action Type", "Stage", "Skip Rate"],
            ["2026-01-01T00:05:00", "Bid", "Five", 0.25],
            ["2026-01-01T00:10:00", "Bid", "Five", 0.5],
        ]
    }

    result = skip_rates_service.process_response(response, parse_datetimes=True)

    assert list(result.columns) == ["GB Local Time", "Action Type", "Stage", "Skip Rate"]
    assert isinstance(result.index, pd.RangeIndex)
    assert result["GB Local Time"].iloc[0] == pd.Timestamp("2026-01-01T00:05:00")
    assert result["Skip Rate"].iloc[0] == 0.25


def test_process_skip_rates_response_preserves_duplicate_gb_local_times():
    response = {
        "data": [
            ["GB Local Time", "Action Type", "Stage", "Skip Rate"],
            ["2026-10-25T01:00:00", "Bid", "Five", 0.25],
            ["2026-10-25T01:00:00", "Bid", "Five", 0.5],
        ]
    }

    result = skip_rates_service.process_response(response, parse_datetimes=True)

    assert result["GB Local Time"].duplicated().any()
    assert list(result["Skip Rate"]) == [0.25, 0.5]


def test_process_skip_rates_response_handles_empty_table():
    result = skip_rates_service.process_response({"data": []})

    assert result.empty


def test_get_skip_rates_data_posts_to_skip_rates_endpoint():
    helper = object.__new__(APIHelper)
    helper.endpoints = SimpleNamespace(SKIP_RATES="https://example.test/api/SkipRates")
    captured = {}

    def fake_post(endpoint, request_body):
        captured["endpoint"] = endpoint
        captured["request_body"] = request_body
        return {
            "data": [
                ["GB Local Time", "Action Type", "Stage", "Skip Rate"],
                ["2026-01-01T00:05:00", "Combined", "Five", 0.75],
            ]
        }

    helper._post_request = fake_post

    result = helper.get_skip_rates_data(date(2026, 1, 1), date(2026, 1, 1), selected_plants=["T_TEST-1"])

    assert captured["endpoint"] == "https://example.test/api/SkipRates"
    assert captured["request_body"]["SelectedPlants"] == ["T_TEST-1"]
    assert result["Skip Rate"].iloc[0] == 0.75


@pytest.mark.asyncio
async def test_get_skip_rates_data_async_posts_to_skip_rates_endpoint():
    helper = object.__new__(APIHelper)
    helper.endpoints = SimpleNamespace(SKIP_RATES="https://example.test/api/SkipRates")
    captured = {}

    async def fake_post_async(endpoint, request_body):
        captured["endpoint"] = endpoint
        captured["request_body"] = request_body
        return {
            "data": [
                ["GB Local Time", "Action Type", "Stage", "Skip Rate"],
                ["2026-01-01T00:05:00", "Offer", "Five", 0.6],
            ]
        }

    helper._post_request_async = fake_post_async

    result = await helper.get_skip_rates_data_async(date(2026, 1, 1), date(2026, 1, 1), action_type="Offer")

    assert captured["endpoint"] == "https://example.test/api/SkipRates"
    assert captured["request_body"]["ActionType"] == "Offer"
    assert result["Skip Rate"].iloc[0] == 0.6
