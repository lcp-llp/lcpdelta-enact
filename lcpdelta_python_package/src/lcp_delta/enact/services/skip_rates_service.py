from datetime import date, datetime

import pandas as pd

from lcp_delta.global_helpers import convert_date_to_iso, is_list_of_strings_or_empty


def generate_request(
    date_from: date | datetime,
    date_to: date | datetime,
    selected_granularity: str = "Day",
    action_type: str = "Combined",
    stage: str = "Five",
    source: str = "Psa",
    selected_zone: list[str] | None = None,
    selected_fuel: list[str] | None = None,
    selected_owners: list[str] | None = None,
    selected_optimisers: list[str] | None = None,
    selected_capacity: list[int] | tuple[int, int] | None = None,
    selected_duration: list[float] | tuple[float, float] | None = None,
    selected_plants: list[str] | None = None,
) -> dict:
    return {
        "From": convert_date_to_iso(date_from),
        "To": convert_date_to_iso(date_to),
        "SelectedGranularity": selected_granularity,
        "ActionType": action_type,
        "Stage": stage,
        "Source": source,
        "SelectedZone": _normalise_string_list(selected_zone, "SelectedZone", ["All Zones"]),
        "SelectedFuel": _normalise_string_list(selected_fuel, "SelectedFuel", ["All Fuels"]),
        "SelectedOwners": _normalise_string_list(selected_owners, "SelectedOwners"),
        "SelectedOptimisers": _normalise_string_list(selected_optimisers, "SelectedOptimisers"),
        "SelectedCapacity": _normalise_pair(selected_capacity, "SelectedCapacity"),
        "SelectedDuration": _normalise_pair(selected_duration, "SelectedDuration"),
        "SelectedPlants": _normalise_string_list(selected_plants, "SelectedPlants"),
    }


def process_response(response: dict, parse_datetimes: bool = False) -> pd.DataFrame:
    data = response["data"]

    if len(data) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(data[1:], columns=data[0])
    if parse_datetimes and "GB Local Time" in df.columns:
        df["GB Local Time"] = pd.to_datetime(df["GB Local Time"])

    return df


def _normalise_string_list(value: list[str] | None, field_name: str, default: list[str] | None = None) -> list[str]:
    if value is None:
        return list(default) if default is not None else []

    if not is_list_of_strings_or_empty(value):
        raise ValueError(f"{field_name} input must be a list of strings")

    return value


def _normalise_pair(value: list | tuple | None, field_name: str) -> list | None:
    if value is None:
        return None

    if not isinstance(value, list | tuple) or len(value) != 2:
        raise ValueError(f"{field_name} input must contain exactly two values")

    return list(value)
