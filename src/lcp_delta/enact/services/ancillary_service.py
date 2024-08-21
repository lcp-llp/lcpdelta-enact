import pandas as pd

from typing import Union
from datetime import date, datetime

from lcp_delta.enact.enums import AncillaryContractGroup


def generate_ancillary_request(
    ancillary_contract_type: str,
    option_one: Union[str, int] | None = None,
    option_two: Union[int, str] | None = None,
    date_requested: datetime | None = None,
    ancillary_contract_group: AncillaryContractGroup | None = None,
) -> dict:
    if date_requested:
        if not isinstance(date_requested, date | datetime):
            raise TypeError("Requested date must be a date or datetime")
        if (
            ancillary_contract_group == AncillaryContractGroup.Dynamic
            or ancillary_contract_group == AncillaryContractGroup.SFfr
        ):
            option_one = "-".join([str(date_requested.month), str(date_requested.year)])
        if ancillary_contract_group == AncillaryContractGroup.StorDayAhead:
            option_one = "-".join([str(date_requested.year), str(date_requested.month), str(date_requested.day)])

    request_body = {
        "AncillaryContractType": ancillary_contract_type,
        "OptionOne": option_one,
    }

    if option_two is not None:
        request_body["OptionTwo"] = option_two

    return request_body


def process_ancillary_response(
    response: dict, ancillary_contract_group: AncillaryContractGroup | None = None
) -> pd.DataFrame:
    print(ancillary_contract_group)
    if "data" not in response or not response["data"]:
        return pd.DataFrame()
    first_item = response["data"][0]
    if ancillary_contract_group == AncillaryContractGroup.SFfr:
        return pd.DataFrame(first_item["plants"])
    if ancillary_contract_group == AncillaryContractGroup.ManFr:
        for entry in first_item["plants"]:
            entry.update(entry.pop("data"))
        df = pd.DataFrame(first_item["plants"])
        if not df.empty:
            unit_column_name = df.columns[0]
            df.set_index(unit_column_name, inplace=True)
        return df
    if ancillary_contract_group == AncillaryContractGroup.StorDayAhead:
        return pd.DataFrame(first_item["plants"])
    if ancillary_contract_group == AncillaryContractGroup.Ffr:
        df = pd.DataFrame(first_item["plants"])
        df.set_index("tenderNumber", inplace=True)
        return df
    if ancillary_contract_group == AncillaryContractGroup.Dynamic:
        return _process_dynamic_response(response)
    return response


def _process_dynamic_response(response: dict) -> pd.DataFrame:
    df = pd.DataFrame(response["data"][0]["plants"])
    if not df.empty:
        df.set_index("orderId", inplace=True)
    return df
