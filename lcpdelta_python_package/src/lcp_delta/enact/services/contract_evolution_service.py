import pandas as pd
from datetime import datetime
from lcp_delta.global_helpers import convert_datetime_to_iso
from lcp_delta.enact.helpers import convert_embedded_list_to_df

def generate_request(
    instrument,
    contract,
    contract_period = None,
    date_from: datetime = None,
    date_to: datetime = None,
) -> dict:
    if date_from is not None:
        date_from = convert_datetime_to_iso(date_from)
    if date_to is not None:
        date_to = convert_datetime_to_iso(date_to)
    return {
        "From": date_from,
        "To": date_to,
        "Instrument": instrument,
        "Contract": contract,
        "ContractPeriod": contract_period,
    }

def process_contract_evolution_response(response: dict) -> pd.DataFrame:
    return convert_embedded_list_to_df(response, "Day")