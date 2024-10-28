import pandas as pd

from datetime import datetime

from lcp_delta.global_helpers import convert_datetime_to_iso


def generate_request(
    fromDate: datetime,
    toDate: datetime | None = None,
    aggregate: bool = False,
    numberOfSimilarDays: int = 10,
    selectedEfaBlocks: int | None = None,
    seriesInput: list[str] = None,
) -> dict:
    fromDateString = convert_datetime_to_iso(fromDate)
    if toDate != None:
        toDateString = convert_datetime_to_iso(toDate)
    else:
        toDateString = None

    return {
        "from": fromDateString,
        "to": toDateString,
        "aggregate": aggregate,
        "numberOfSimilarDays": numberOfSimilarDays,
        "selectedEfaBlocks": selectedEfaBlocks,
        "seriesInput": seriesInput,
    }


def process_response(response: dict) -> pd.DataFrame:
    output: dict[int, pd.DataFrame] = {}
    for key, value in response["data"].items():
        data_list = []
        for item in value:
            day = item["day"]
            score = item["score"]
            raw_data = item["rawData"]
            raw_data["day"] = day
            raw_data["score"] = score
            data_list.append(raw_data)
        df = pd.DataFrame(data_list)
        if not df.empty:
            df.set_index("score", inplace=True)
        output[int(key)] = df

    return output
