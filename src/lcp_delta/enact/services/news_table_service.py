import pandas as pd

from lcp_delta.enact.helpers import convert_embedded_list_to_df


def generate_request(table_id: str) -> dict:
    if table_id.lower() == "lcp":
        table_id = "Lcp"
    return {
        "TableId": table_id,
    }


def process_response(response: dict) -> pd.DataFrame:
    return convert_embedded_list_to_df(response)
