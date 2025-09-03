import pandas as pd

from datetime import datetime

from lcp_delta.global_helpers import get_period, convert_datetime_to_iso
from lcp_delta.enact.helpers import convert_response_to_df, convert_dict_to_df


def generate_contract_id_request(contract_id: str) -> dict:
    return {"ContractId": contract_id}


def generate_time_and_type_request(type: str, date: datetime, period: int = None) -> dict:
    period = get_period(date, period)
    date = convert_datetime_to_iso(date)
    return {"Type": type, "Date": date, "Period": period}


def process_trades_response(response: dict) -> pd.DataFrame:
    return convert_response_to_df(response, index_on=-1)


def process_order_book_response(response: dict) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
    for table_str in response["data"].keys():
        output[table_str] = convert_response_to_df(response, nested_key=table_str)

    return output


def generate_contract_request(date: datetime) -> dict:
    date = convert_datetime_to_iso(date)
    return {"Date": date}


def process_contract_response(response: dict) -> pd.DataFrame:
    return convert_response_to_df(response)
