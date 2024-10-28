import pandas as pd
from datetime import datetime


def generate_request(
    date_from: datetime,
    date_to: datetime,
    countries: list[str],
    products: list[str],
    directions: list[str],
    market: str,
    metrics: list[str],
    aggregation_types: list[str],
    granularity: str,
    weighting_metric: list[str] = None,
) -> dict:
    dates = [
        {
            "fromDay": date_from.day,
            "fromMonth": date_from.month,
            "fromYear": date_from.year,
            "toDay": date_to.day,
            "toMonth": date_to.month,
            "toYear": date_to.year,
        }
    ]

    request_body = {
        "Country": countries,
        "Product": products,
        "Direction": directions,
        "Metric": metrics,
        "Market": market,
        "SummaryMetric": aggregation_types,
        "Granularity": granularity,
        "Dates": dates,
    }

    if weighting_metric:
        request_body["WeightingMetric"] = weighting_metric

    return request_body


def process_response(response: dict) -> pd.DataFrame | dict:
    try:
        df = pd.DataFrame(response["data"]["dictionaryOutput"])
        first_key = next(iter(response["data"]["dictionaryOutput"]))
        return df.set_index(first_key)

    except (ValueError, TypeError, IndexError):
        return response
