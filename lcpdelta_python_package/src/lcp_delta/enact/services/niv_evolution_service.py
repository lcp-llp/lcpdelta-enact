from datetime import datetime
from lcp_delta.global_helpers import convert_date_to_iso
import pandas as pd
import re

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

    for datetime, option_dict in data.items():
        for option, values in option_dict.items():
            rows.append({
                "Time Stamp (GMT)": datetime,
                "Option": option,
                "Evolution Data": values
            })

    return pd.DataFrame(rows, columns=["Time Stamp (GMT)", "Option", "Evolution Data"])
