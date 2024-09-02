from typing import Any

from lcp_delta.global_helpers import is_list_of_strings_or_empty
from lcp_delta.enact.helpers import convert_response_to_df


def generate_series_data_request(
    series_id: str,
    date_from: str,
    date_to: str,
    country_id: str,
    option_id: list[str],
    half_hourly_average: bool,
    request_time_zone_id: str | None = None,
    time_zone_id: str | None = None,
) -> dict:
    if option_id is not None:
        if not is_list_of_strings_or_empty(option_id):
            raise ValueError("Option ID input must be a list of strings")

    request_body = {
        "SeriesId": series_id,
        "CountryId": country_id,
        "From": date_from,
        "To": date_to,
        "OptionId": option_id,
        "halfHourlyAverage": half_hourly_average,
    }

    if request_time_zone_id is not None:
        request_body["requestTimeZoneId"] = request_time_zone_id

    if time_zone_id is not None:
        request_body["timeZoneId"] = time_zone_id

    return request_body


def process_series_data_response(response: dict, parse_datetimes=False):
    try:
        return convert_response_to_df(response, parse_datetimes, nested_key="data")
    except (ValueError, TypeError, IndexError):
        return response


def generate_series_info_request(series_id: str, country_id: str | None):
    request_body = {"SeriesId": series_id}
    if country_id is not None:
        request_body["CountryId"] = country_id

    return request_body


def generate_multi_series_data_request(
    series_ids: list[str],
    date_from: str,
    date_to: str,
    country_id: str,
    option_ids: list[str],
    half_hourly_average: bool,
    request_time_zone_id: str | None = None,
    time_zone_id: str | None = None,
) -> dict:
    if option_ids is not None:
        if not is_list_of_strings_or_empty(option_ids):
            raise ValueError("Option ID input must be a list of strings")

    request_body = {
        "SeriesIds": series_ids,
        "CountryId": country_id,
        "From": date_from,
        "To": date_to,
        "OptionIds": option_ids,
        "halfHourlyAverage": half_hourly_average,
    }

    if request_time_zone_id is not None:
        request_body["requestTimeZoneId"] = request_time_zone_id

    if time_zone_id is not None:
        request_body["timeZoneId"] = time_zone_id

    return request_body
