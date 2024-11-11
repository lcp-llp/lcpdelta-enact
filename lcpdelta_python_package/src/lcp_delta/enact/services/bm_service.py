import pandas as pd
import json

from datetime import datetime

from lcp_delta.global_helpers import get_period, convert_datetime_to_iso
from lcp_delta.enact.helpers import convert_response_to_df


def generate_by_period_request(date: datetime, period: int = None, include_accepted_times: bool = False):
    period = get_period(date, period)
    date = convert_datetime_to_iso(date)

    request_body = {"Date": date, "Period": period}
    if include_accepted_times is not False:
        request_body["includeAcceptedTimes"] = "True"

    return request_body


def process_by_period_response(response: dict):
    output: dict[str, pd.DataFrame] = {}
    df_columns = ["acceptedBids", "acceptedOffers", "tableOffers", "tableBids"]

    for key_str in df_columns:
        if key_str in response["data"]:
            df = convert_response_to_df(response, nested_key=key_str)
            expand_bsad_metadata(df)
            output[key_str] = df

    return output


def generate_by_search_request(date: datetime, option: str, search_string: str | None, include_accepted_times: bool):
    date = convert_datetime_to_iso(date)

    request_body = {"Date": date, "Option": option, "SearchString": search_string}
    if include_accepted_times is not False:
        request_body["includeAcceptedTimes"] = "True"

    return request_body


def process_by_search_response(response: dict):
    return pd.DataFrame(response["data"][1:], columns=response["data"][0])


def expand_bsad_metadata(df: pd.DataFrame):
    bsad_rows = df.index.str.contains("BSAD_")
    if bsad_rows.any():
        bsad_additional_data = df.loc[bsad_rows, "additionalBsadData"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )

        df.loc[bsad_rows, "bsadAssetId"] = bsad_additional_data.apply(
            lambda x: x.get("plantId") if isinstance(x, dict) else None
        )
        df.loc[bsad_rows, "bsadPartyName"] = bsad_additional_data.apply(
            lambda x: x.get("partyName") if isinstance(x, dict) else None
        )
        df.loc[bsad_rows, "bsadFuelType"] = bsad_additional_data.apply(
            lambda x: x.get("fuelType") if isinstance(x, dict) else None
        )
