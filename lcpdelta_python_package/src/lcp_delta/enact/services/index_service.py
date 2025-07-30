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

def generate_index_info_request(
    index_id: str,
) -> dict:
    return {
        "IndexId": index_id,
    }

def generate_gb_request(
    date_from: datetime,
    date_to: datetime,
    index_id: str,
    normalisation="EuroPerKwPerYear",
    granularity="Week",
    show_profit = "false",
    gas_price_assumption = "DayAheadForward",
    market_price_assumption = "WeightedAverageDayAheadPrice",
    account_for_availability_in_normalisation = "false",
    include_wholesale_split = "false",
    bm_split_out_option = None,
    ancillary_revenue_type = "ByProduct",
    group_dx = "false",
    include_capacity_market = "true",
    include_non_delivery_charges = "true",
    include_imbalance=False,
    include_estimated_charging_cost=False,
    include_fpnflagoff_wholesale=False,
    charging_cost_price=None,
    charging_cost_assumption=None
) -> dict:
    date_from, date_to = convert_datetimes_to_iso(date_from, date_to)
    return {
        "From": date_from,
        "To": date_to,
        "IndexId": index_id,
        "SelectedNormalisation": normalisation,
        "SelectedGranularity": granularity,
        "ShowProfit": show_profit,
        "GasPriceAssumption": gas_price_assumption,
        "MarketPriceAssumption": market_price_assumption,
        "AccountForAvailabilityInNormalisation": account_for_availability_in_normalisation,
        "IncludeWholesaleSplit": include_wholesale_split,
        "BmSplitOutOption": bm_split_out_option,
        "AncillaryProfitAggregation": ancillary_revenue_type,
        "GroupDx": group_dx,
        "IncludeCmRevenues": include_capacity_market,
        "IncludeNonDeliveryCharges": include_non_delivery_charges,
        "IncludeImbalance": include_imbalance,
        "IncludeEstimatedChargingCost": include_estimated_charging_cost,
        "ChargingCostPrice": charging_cost_price,
        "ChargingCostAssumption": charging_cost_assumption,
        "IncludeFpnFlagOffWholesale": include_fpnflagoff_wholesale
    }

def process_index_info_response(response: dict) -> pd.DataFrame:
    return convert_embedded_list_to_df(response)

def process_index_data_response(response: dict) -> pd.DataFrame:
    return convert_embedded_list_to_df(response, "Day")
