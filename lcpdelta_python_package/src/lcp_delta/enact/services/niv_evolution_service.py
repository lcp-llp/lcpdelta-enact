from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from lcp_delta.global_helpers import convert_date_to_iso, get_period_from_datetime
import pandas as pd

def generate_by_period_request(period: int, date: datetime, options: list[str]):
    date_str = convert_date_to_iso(date)
    return {"date": date_str, "period": period, "options": options}

def generate_by_day_request(date: datetime, options: list[str]):
    date_str = convert_date_to_iso(date)
    return {"date": date_str, "options": options}

def generate_date_range_request(start_date: datetime, end_date: datetime, options: list[str], cursor:str = None):
    start_date_str = convert_date_to_iso(start_date)
    end_date_str = convert_date_to_iso(end_date)
    request_body = {
        "start": start_date_str,
        "end": end_date_str,
        "options": options
    }
    if cursor:
        request_body["cursor"] = cursor

    return request_body

def process_response(response: dict) -> pd.DataFrame:
    return pd.DataFrame(response["data"][1:], columns=response["data"][0])
