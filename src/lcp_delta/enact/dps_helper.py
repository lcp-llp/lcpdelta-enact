import time as pytime
import pandas as pd

from datetime import datetime as dt
from functools import partial
from typing import Callable
from signalrcore.hub_connection_builder import HubConnectionBuilder

from lcp_delta.global_helpers import is_list_of_strings_or_empty
from lcp_delta.enact.api_helper import APIHelper


class DPSHelper:
    def __init__(self, username: str, public_api_key: str):
        self.api_helper = APIHelper(username, public_api_key)
        self.__initialise()
        self.last_updated_header = "DateTimeLastUpdated"

    def __initialise(self):
        self.enact_credentials = self.api_helper.credentials_holder
        self.data_by_subscription_id: dict[str, tuple[Callable[[str], None], pd.DataFrame, bool]] = {}
        access_token_factory = partial(self._fetch_bearer_token)
        self.hub_connection = (
            HubConnectionBuilder()
            .with_url(
                "https://enact-signalrhub.azurewebsites.net/dataHub",
                options={"access_token_factory": access_token_factory},
            )
            .with_automatic_reconnect(
                {"type": "raw", "keep_alive_interval": 10, "reconnect_interval": 5, "max_attempts": 5}
            )
            .build()
        )

        self.hub_connection.on_open(lambda: print("connection opened and handshake received ready to send messages"))
        self.hub_connection.on_close(lambda: print("connection closed"))

        success = self.hub_connection.start()
        pytime.sleep(5)

        if not success:
            raise ValueError("connection failed")

    def _fetch_bearer_token(self):
        return self.enact_credentials.bearer_token

    def _add_subscription(self, request_object: list[dict[str, str]], subscription_id: str):
        self.hub_connection.send(
            "JoinEnactPush", request_object, lambda m: self._callback_received(m.result, subscription_id)
        )

    def subscribe_to_notifications(self, handle_notification_method: Callable[[str], None]):
        self.hub_connection.send(
            "JoinParentCompanyNotificationPush",
            [],
            on_invocation=lambda m: self.hub_connection.on(
                m.result["data"]["pushName"], lambda x: handle_notification_method(x)
            ),
        )

    def _initialise_series_subscription_data(
        self,
        series_id: str,
        country_id: str,
        option_id: list[str],
        handle_data_method: Callable[[str], None],
        parse_datetimes: bool,
    ):
        now = dt.now()
        day_start = dt(now.year, now.month, now.day, tzinfo=now.tzinfo)
        initial_series_data = self.api_helper.get_series_data(
            series_id, day_start, now, country_id, option_id, parse_datetimes=parse_datetimes
        )
        initial_series_data[self.last_updated_header] = now
        self.data_by_subscription_id[self.__get_subscription_id(series_id, country_id, option_id)] = (
            handle_data_method,
            initial_series_data,
            parse_datetimes,
        )

    def _callback_received(self, m, subscription_id: str):
        self.hub_connection.on(m["data"]["pushName"], lambda x: self._process_push_data(x, subscription_id))

    def _process_push_data(self, data, subscription_id):
        (user_callback, all_data, parse_datetimes) = self.data_by_subscription_id[subscription_id]
        modified_data = self._handle_new_series_data(all_data, data, parse_datetimes)
        user_callback(modified_data)

    def _handle_new_series_data(
        self, all_data: pd.DataFrame, data_push_holder: list, parse_datetimes: bool
    ) -> pd.DataFrame:
        try:
            if all_data.empty:
                return data_push_holder
            data_push = data_push_holder[0]["data"]
            push_ids = list(all_data.columns)[:-1]  # Exclude last updated column
            pushes = data_push["data"]
            for push in pushes:
                push_current = push["current"]
                push_date_time = f'{push_current["datePeriod"]["datePeriodCombinedGmt"]}'
                if push_date_time[-1:] != "Z":
                    push_date_time += "Z"

                if parse_datetimes:
                    push_date_time = pd.to_datetime(push_date_time, utc=True)

                push_values = (
                    push_current["arrayPoint"][1:]
                    if not push["byPoint"]
                    else list(push_current["objectPoint"].values())
                )

                for index, push_id in enumerate(push_ids):
                    push_value = push_values[index]
                    all_data.loc[push_date_time, push_id] = push_value
                    all_data.loc[push_date_time, self.last_updated_header] = dt.now()
            return all_data
        except Exception:
            return data_push_holder

    def terminate_hub_connection(self):
        self.hub_connection.stop()

    def subscribe_to_epex_trade_updates(self, handle_data_method: Callable[[str], None]) -> None:
        """
        `THIS FUNCTION IS IN BETA`

        Subscribe to EPEX trade updates and specify a callback function to handle the received data.

        Parameters:
            handle_data_method `Callable`: A callback function that will be invoked with the received EPEX trade updates.
                The function should accept one argument, which will be the data received from the EPEX trade updates.
        """
        # Create the Enact request object for EPEX trade updates
        enact_request_object_epex = [{"Type": "EPEX", "Group": "Trades"}]
        # Add the subscription for EPEX trade updates with the specified callback function
        self._add_subscription(enact_request_object_epex, handle_data_method)

    def subscribe_to_series_updates(
        self,
        handle_data_method: Callable[[str], None],
        series_id: str,
        option_id: list[str] = None,
        country_id="Gb",
        parse_datetimes: bool = False,
    ) -> None:
        """
        Subscribe to series updates with the specified SeriesId and optional parameters.

        Parameters:
            handle_data_method `Callable`: A callback function that will be invoked with the received series updates.
                The function should accept one argument, which will be the data received from the series updates.

            series_id `str`: This is the Enact ID for the requested Series, as defined in the query generator on the "General" tab.

            option_id `list[str]` (optional): If the selected Series has options, then this is the Enact ID for the requested Option,
                                       as defined in the query generator on the "General" tab.
                                       If this is not sent, but is required, you will receive back an error.

            country_id `str` (optional): This is the Enact ID for the requested Country, as defined in the query generator on the "General" tab. Defaults to "Gb".

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.
        """
        request_details = {"SeriesId": series_id, "CountryId": country_id}

        if option_id:
            if not is_list_of_strings_or_empty(option_id):
                raise ValueError("Option ID input must be a list of strings")
            request_details["OptionId"] = option_id
        subscription_id = self.__get_subscription_id(series_id, country_id, option_id)
        if subscription_id in self.data_by_subscription_id:
            return
        (handle_data_old, initial_data_from_series_api, parse_datetimes_old) = self.data_by_subscription_id.get(
            subscription_id, (None, pd.DataFrame(), False)
        )
        if initial_data_from_series_api.empty:
            self._initialise_series_subscription_data(
                series_id, country_id, option_id, handle_data_method, parse_datetimes
            )
        else:
            self.data_by_subscription_id[subscription_id][0] = handle_data_method

        enact_request_object_series = [request_details]
        self._add_subscription(enact_request_object_series, subscription_id)

    def __get_subscription_id(self, series_id: str, country_id: str, option_id: list[str]) -> str:
        subscription_id = (series_id, country_id)
        if option_id:
            subscription_id += tuple(option_id)
        return subscription_id
