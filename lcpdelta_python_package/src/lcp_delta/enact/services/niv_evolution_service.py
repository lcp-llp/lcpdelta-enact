from datetime import datetime

import pandas as pd

from lcp_delta.global_helpers import convert_date_to_iso


COLUMNS = ["Date", "Period", "Metric", "Timestamp", "Value"]


def _validate_options(options: list[str]):
    if any(option.casefold() in {"systemprice", "1"} for option in options):
        raise ValueError("SystemPrice is not supported for NIV evolution because backend data collection is disabled.")


def generate_by_period_request(period: int, date: datetime, options: list[str]):
    _validate_options(options)
    return {"date": convert_date_to_iso(date), "period": period, "options": options}


def generate_by_day_request(date: datetime, options: list[str]):
    _validate_options(options)
    return {"date": convert_date_to_iso(date), "options": options}


def generate_date_range_request(
    start_date: datetime, end_date: datetime, options: list[str], cursor: str | None = None
):
    _validate_options(options)
    request_body = {"end": convert_date_to_iso(end_date), "options": options}
    if cursor:
        request_body["cursor"] = cursor
    else:
        request_body["start"] = convert_date_to_iso(start_date)
    return request_body


def process_response(response: dict) -> pd.DataFrame:
    """Keep the API's column order, date strings and minute-by-minute rows."""
    if response is None:
        raise ValueError("NIV evolution API returned null instead of a data response.")
    data = response["data"]
    if data is None:
        raise ValueError("NIV evolution API returned null data.")
    if not data:
        return pd.DataFrame(columns=COLUMNS)
    return pd.DataFrame(data[1:], columns=data[0])
