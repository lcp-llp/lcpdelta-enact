import pandas as pd
from lcp_delta.global_helpers import convert_datetime_to_iso

def generate_request(df):
    validate_dataframe(df)

    return {
        "Data": [
            {
                "DateTimeUtc": convert_datetime_to_iso(dt),
                "DynamicFrequencyResponseCreditedVolumeMw": float(v1),
                "TotalCreditedVolumeMwh": float(v2),
            }
            for dt, v1, v2 in zip(
                df["date_time_utc"],
                df["dynamic_frequency_response_credited_volume_mw"],
                df["total_credited_volume_mwh"]
            )
        ]
    }
    
def validate_dataframe(df):
    required_cols = [
        "date_time_utc",
        "dynamic_frequency_response_credited_volume_mw",
        "total_credited_volume_mwh",
    ]
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def process_response(response):
    weekly_df = build_df_from_table(response["weeklySummary"])
    monthly_df = build_df_from_table(response["monthlySummary"])
    period_metrics_df = build_df_from_table(response["periodMetrics"])
    full_time_range_metrics_df = build_df_from_table(response["fullTimeRangeMetrics"])
    
    return {
        "weekly": weekly_df,
        "monthly": monthly_df,
        "period_metrics": period_metrics_df,
        "full_range_metrics": full_time_range_metrics_df,
    }

def build_df_from_table(table):
    return pd.DataFrame(table['rows'], columns=table['columns'])
        