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
    data = response["data"]
    rows = []

    for datetime_str, option_dict in data.items():
        dt_gmt = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        dt_gb = dt_gmt.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/London"))
        period = get_period_from_datetime(dt_gb)
        date = dt_gb.date()
        values_start = dt_gb - timedelta(minutes=90)
        for option, values in option_dict.items():
            for index, value in enumerate(values):
                rows.append({
                    "Date": date,
                    "Period": period,
                    "Evolution Metric": option,
                    "Time Stamp": values_start + timedelta(minutes=index),
                    "Value": value
                })

    return pd.DataFrame(rows, columns=["Date", "Period", "Evolution Metric", "Time Stamp", "Value"])
