import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.figure as fig

from lcp_delta.common import CredentialsHolder
from lcp_delta.enact.api_helper import APIHelper


class ChartHelper:
    def __init__(self, username: str, public_api_key: str):
        self.enact_credentials = CredentialsHolder(username, public_api_key)
        self.api_helper = APIHelper(username, public_api_key)

    def plot_series_data(self, series_data: pd.DataFrame, series_id: str, country_id: str = None) -> fig.Figure:
        """Converts series data in pandas `DataFrame` format to a plottable matplotlib `Figure`.

        This method takes in a "DataFrame" containing Enact series data and the corresponding series ID, retrieved information associated with the series and returns a `Figure` object. The figure may be plotted by calling "figure.show()".

        Args:
            series_data `DataFrame`: This is the series data stored in a pandas `DataFrame`. This may be obtained by calling "get_series_data()" on an instance of `APIHelper`.

            series_id `str`: This is the Enact ID for the requested series, as defined in the query generator on the "General" tab.

            country_id `str` (optional): The country ID for filtering the data.

        """

        series_info = self.api_helper.get_series_info(series_id, country_id)["data"]

        country = series_info["countries"][0]["name"] if len(series_info["countries"]) else ""
        plot_title = series_info["name"]
        if country:
            plot_title = f"{plot_title}: {country}"
        units = series_info["suffix"]

        plt.ioff()
        fig, ax = plt.subplots()
        series_data.plot(ax=ax)
        ax.set_title(plot_title)
        ax.set_ylabel(units)

        return fig
