import pandas as pd
from datetime import datetime
from lcp_delta.global_helpers import convert_date_to_iso, convert_datetime_to_iso

def generate_jao_bids_request(corridor: str, horizon: str, dayCET: datetime, bidPeriodStart: datetime = None):

    dayCET_str = convert_date_to_iso(dayCET)
    
    request_body = {"Corridor": corridor, "Horizon": horizon, "DayCET": dayCET_str}
    if bidPeriodStart != None:
        bidPeriodStart_str = convert_datetime_to_iso(bidPeriodStart)
        request_body["BidPeriodStart"] = bidPeriodStart_str

    return request_body

def process_response(response: dict) -> pd.DataFrame:

    df = pd.DataFrame(response.get("data", []))

    if "dayCET" in df.columns:
        df["dayCET"] = pd.to_datetime(df["dayCET"])
    if "bidPeriodStart" in df.columns:
        df["bidPeriodStart"] = pd.to_datetime(df["bidPeriodStart"])

    return df