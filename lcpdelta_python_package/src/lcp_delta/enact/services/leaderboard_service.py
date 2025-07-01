import pandas as pd

from datetime import datetime

from lcp_delta.global_helpers import convert_datetimes_to_iso
from lcp_delta.enact.helpers import convert_embedded_list_to_df


def generate_request_v1(
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


def generate_request_v2(
    date_from: datetime,
    date_to: datetime,
    type="Plant",
    revenue_metric="PoundPerMwPerH",
    market_price_assumption="WeightedAverageDayAheadPrice",
    gas_price_assumption="DayAheadForward",
    include_capacity_market_revenues=False,
    ancillary_profit_aggregation="FrequencyAndReserve",
    group_dx=False,
    aggregate=None,
    show_co_located_fuels=False,
    account_for_availability_in_normalisation=False,
    fuels=None,
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
        "Type": type,
        "RevenueMetric": revenue_metric,
        "MarketPriceAssumption": market_price_assumption,
        "GasPriceAssumption": gas_price_assumption,
        "IncludeCmRevenues": include_capacity_market_revenues,
        "AncillaryProfitAggregation": ancillary_profit_aggregation,
        "GroupDx": group_dx,
        "Aggregate": aggregate,
        "ShowCoLocatedFuels": show_co_located_fuels,
        "AccountForAvailabilityInNormalisation": account_for_availability_in_normalisation,
        "Fuels": fuels,
        "IncludeImbalance": include_imbalance,
        "IncludeEstimatedChargingCost": include_estimated_charging_cost,
        "IncludeFpnFlagOffWholesale": include_fpnflagoff_wholesale,
        "ChargingCostPrice": charging_cost_price,
        "ChargingCostAssumption": charging_cost_assumption
    }


def process_response(response: dict, type: str) -> pd.DataFrame:
    index = "Plant - Owner" if type == "Owner" else "Plant - ID"
    return convert_embedded_list_to_df(response, index)
