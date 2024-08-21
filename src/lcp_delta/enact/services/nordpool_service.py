from datetime import datetime

from lcp_delta.global_helpers import convert_datetime_to_iso


def generate_request(date: datetime) -> dict:
    date = convert_datetime_to_iso(date)
    return {"Date": date}


def process_response():
    pass
