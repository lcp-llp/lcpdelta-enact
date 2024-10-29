import pandas as pd
import calendar

from lcp_delta.global_helpers import parse_df_datetimes


def convert_response_to_df(
    response: dict, parse_datetimes: bool = False, index_on: int = 0, key: str = "data", nested_key: str = None
) -> pd.DataFrame:
    data = response[key]
    if nested_key:
        data = data[nested_key]

    if isinstance(data, list):
        df = pd.DataFrame(data)
        if len(df.columns) > 0:
            df.set_index(df.columns[index_on], inplace=True)
    elif isinstance(data, dict):
        df = convert_dict_to_df(data, parse_datetimes, index_on)
    else:
        raise ValueError(f"Unexpected response data type: {type(data)}. Expected 'list' or 'dict'.")

    return df


def convert_dict_to_df(data: dict, parse_datetimes: bool = False, index_on: int = 0) -> pd.DataFrame:
    df = pd.DataFrame(data)
    if not df.empty:
        df.set_index(df.columns[index_on], inplace=True)
        if parse_datetimes:
            parse_df_datetimes(df, parse_index=True, inplace=True)

    return df


def convert_embedded_list_to_df(data: list, index: str = None, key: str = "data") -> pd.DataFrame:
    if key:
        data = data[key]
    df = pd.DataFrame(data[1:], columns=data[0])
    if index:
        df = df.set_index(index)

    return df


def get_month_name(month: int):
    if not 0 < month <= 12:
        raise ValueError("Month must be an integer less than 12")

    return calendar.month_name[month]
