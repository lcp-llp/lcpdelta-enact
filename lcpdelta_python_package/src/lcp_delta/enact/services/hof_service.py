import pandas as pd

from datetime import datetime, timedelta

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


def get_nearest_forecast_for_horizon(
    forecast_target_time: datetime, horizon_offset: timedelta, forecasts: pd.DataFrame
):
    horizon_time = forecast_target_time - horizon_offset
    forecast_issue_times = pd.to_datetime(forecasts.columns).tz_localize(None)
    closest_forecast_index = forecast_issue_times.get_indexer([horizon_time], method="nearest")[0]
    return forecasts.iloc[forecasts.index.get_loc(forecast_target_time), closest_forecast_index]


def process_response_for_time_horizon(response: dict, horizons: list[int]) -> pd.DataFrame:
    formatted_response = process_date_range_response(response)
    forecasts = list(formatted_response.values())

    if not all(isinstance(item, int) for item in horizons):
        raise ValueError("Horizon input must be a list of integers")

    horizon_forecasts_combined = pd.DataFrame()
    for forecast in forecasts:
        forecast.index = pd.to_datetime(forecast.index).tz_localize(None)

        horizon_data = {"Time": forecast.index}

        for horizon in horizons:
            horizon_data[f"T-{horizon}m Forecast"] = forecast.index.map(
                lambda x: get_nearest_forecast_for_horizon(x, pd.Timedelta(minutes=horizon), forecast)
            )

        horizon_forecast = pd.DataFrame(horizon_data)
        horizon_forecasts_combined = pd.concat([horizon_forecasts_combined, horizon_forecast])

    horizon_forecasts_combined.set_index("Time", inplace=True)

    return horizon_forecasts_combined
