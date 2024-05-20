from datetime import datetime
import pandas as pd

from ..common import APIHelperBase


class APIHelper(APIHelperBase):
    # Series:
    def get_exporter_data(
        self,
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
    ) -> pd.DataFrame:
        """Get historic data for selected ancillary services.

        This method retrieves data for specific ancillary services. The time range, country, ancillary products, ancillary product directions and ancillary market type must all be specified.

        Args:
            date_from `datetime.datetime`: This is the start of the date-range being requested.

            date_to `datetime.datetime`: This is the end of the date-range being requested. If a single day is wanted, then this will be the same as the From value.

            countries `list[str]`: The list of countries data is being requested for.

            products `list[str]`: The ancillary products data is being requested for.

            directions `list[str]`: The regulation directions for the requested products. These may be any or all of "Upward", "Downward" or "Symmetric".

            market `str`: The type of market data is being requested for. This will be either "Availability" or "Utilisation".

            metrics `list[str]`: The type of data being requested. This will be one or both of "Volume" and "Price".

            aggregation_types `list[str]`: How data should be aggregated (where possible). This will either be "Average" or "Sum".

            granularity `str`: The requested granularity of data that is returned.

            weighting_metric `list[str]` (optional): How to weight the requested metrics. This parameter is optional, and must be the same length as `metrics` if not set to `None`.

        Note that the arguments required for specific enact data can be found on the site.

        Returns:
            Response: The response object containing the series data.
        """
        endpoint = "https://enact-staticchartapi.azurewebsites.net/FLEXTrackAPI/Exporter/Data"

        from_year = date_from.year
        from_month = date_from.month
        from_day = date_from.day
        to_year = date_to.year
        to_month = date_to.month
        to_day = date_to.day

        dates = [
            {
                "fromDay": from_day,
                "fromMonth": from_month,
                "fromYear": from_year,
                "toDay": to_day,
                "toMonth": to_month,
                "toYear": to_year,
            }
        ]

        request_details = {
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
            request_details["WeightingMetric"] = weighting_metric

        response = self.post_request(endpoint, request_details)

        try:
            df = pd.DataFrame(response["data"]["dictionaryOutput"])
            first_key = next(iter(response["data"]["dictionaryOutput"]))
            df = df.set_index(first_key)

            return df
        except (ValueError, TypeError, IndexError):
            return response
