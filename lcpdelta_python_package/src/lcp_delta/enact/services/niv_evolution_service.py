from datetime import datetime
from lcp_delta.global_helpers import convert_date_to_iso
import pandas as pd
import re

def generate_by_period_request(period: int, date: datetime, options: list[str]):

    date_str = convert_date_to_iso(date)
    request_body = {"date": date_str, "period": period, "options": options}

    return request_body

def generate_by_day_request(date: datetime, options: list[str]):

    date_str = convert_date_to_iso(date)
    request_body = {"date": date_str, "options": options}

    return request_body

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

    for date_period, option_dict in data.items():
        date_str, period_str = date_period.split(" - ")
        date = datetime.strptime(date_str.strip(), "%d/%m/%Y %H:%M:%S").date()
        period = int(period_str.strip())

        for option, values in option_dict.items():
            rows.append({
                "Date": date,
                "Period": period,
                "Option": option,
                "Evolution Data": values
            })

    df = pd.DataFrame(rows, columns=["Date", "Period", "Option", "Evolution Data"])
    return df
