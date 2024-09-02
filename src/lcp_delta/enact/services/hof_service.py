import pandas as pd

from datetime import datetime

from lcp_delta.global_helpers import convert_datetime_to_iso, convert_datetimes_to_iso, is_list_of_strings_or_empty
from lcp_delta.enact.helpers import convert_dict_to_df, convert_response_to_df


def generate_single_date_request(
    series_id: str,
    date: datetime,
    country_id: str,
    option_id: list[str],
) -> dict:
    date = convert_datetime_to_iso(date)

    request_body = {
        "SeriesId": series_id,
        "CountryId": country_id,
        "Date": date,
    }

    if option_id is not None:
        if not is_list_of_strings_or_empty(option_id):
            raise ValueError("Option ID input must be a list of strings")
        request_body["OptionId"] = option_id

    return request_body


def process_single_date_response(response):
    return convert_response_to_df(response, nested_key="data")


def generate_date_range_request(
    series_id: str,
    date_from: datetime,
    date_to: datetime,
    country_id: str,
    option_id: list[str],
) -> dict:
    date_from, date_to = convert_datetimes_to_iso(date_from, date_to)

    request_body = {"SeriesId": series_id, "CountryId": country_id, "From": date_from, "To": date_to}

    if option_id is not None:
        if not is_list_of_strings_or_empty(option_id):
            raise ValueError("Option ID input must be a list of strings")
        request_body["OptionId"] = option_id

    return request_body


def process_date_range_response(response: dict) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
    for date_str, data in response["data"]["data"].items():
        df = convert_dict_to_df(data)
        output[date_str] = df

    return output


def generate_latest_forecast_request(
    series_id: str,
    date_from: datetime,
    date_to: datetime,
    country_id: str,
    forecast_as_of: datetime,
    option_id: list[str],
) -> dict:
    date_from, date_to = convert_datetimes_to_iso(date_from, date_to)
    forecast_as_of = convert_datetime_to_iso(forecast_as_of)

    request_body = {
        "SeriesId": series_id,
        "CountryId": country_id,
        "From": date_from,
        "To": date_to,
        "ForecastAsOf": forecast_as_of,
    }

    if option_id is not None:
        if not is_list_of_strings_or_empty(option_id):
            raise ValueError("Option ID input must be a list of strings")
        request_body["OptionId"] = option_id

    return request_body


def process_latest_forecast_response(response: dict) -> dict:
    output: dict[str, pd.DataFrame] = {}
    for date_str, data in response["data"]["data"].items():
        if data is None:
            continue
        output[date_str] = convert_dict_to_df(data)

    return output
