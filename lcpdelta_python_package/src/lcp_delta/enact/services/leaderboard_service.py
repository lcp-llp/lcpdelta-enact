import pandas as pd

from datetime import datetime

from lcp_delta.global_helpers import convert_datetimes_to_iso
from lcp_delta.enact.helpers import convert_embedded_list_to_df


def generate_request(
    date_from: datetime,
    date_to: datetime,
    type="Plant",
    revenue_metric="PoundPerMwPerH",
    market_price_assumption="WeightedAverageDayAheadPrice",
    gas_price_assumption="DayAheadForward",
    include_capacity_market_revenues=False,
) -> dict:
    date_from, date_to = convert_datetimes_to_iso(date_from, date_to)
    return {
        "From": date_from,
        "To": date_to,
        "Type": type,
        "RevenueMetric": revenue_metric,
        "MarketPriceAssumption": market_price_assumption,
        "GasPriceAssumption": gas_price_assumption,
        "IncludeCmRevenues": include_capacity_market_revenues,
    }


def process_response(response: dict, type: str) -> pd.DataFrame:
    index = "Plant - Owner" if type == "Owner" else "Plant - ID"
    return convert_embedded_list_to_df(response, index)
