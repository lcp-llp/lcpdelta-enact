import pandas as pd

from datetime import datetime

from lcp_delta.global_helpers import convert_datetimes_to_iso
from lcp_delta.enact.helpers import convert_embedded_list_to_df


def generate_request(
    date_from: datetime,
    date_to: datetime,
    index_id: str,
    normalisation="EuroPerKwPerYear",
    granularity="Week",
) -> dict:
    date_from, date_to = convert_datetimes_to_iso(date_from, date_to)
    return {
        "From": date_from,
        "To": date_to,
        "IndexId": index_id,
        "SelectedNormalisation": normalisation,
        "SelectedGranularity": granularity,
    }


def process_default_index_info_response(response: dict) -> pd.DataFrame:
    return convert_embedded_list_to_df(response)

def process_index_data_response(response: dict) -> pd.DataFrame:
    return convert_embedded_list_to_df(response, "Day")
