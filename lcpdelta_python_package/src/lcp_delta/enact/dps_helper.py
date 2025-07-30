import time as pytime
import pandas as pd
import threading
from datetime import datetime as dt
from functools import partial
from typing import Callable
from signalrcore.hub_connection_builder import HubConnectionBuilder
from lcp_delta.global_helpers import is_list_of_strings_or_empty, is_2d_list_of_strings
import lcp_delta.enact.endpoints as ep
from lcp_delta.enact.api_helper import APIHelper
from lcp_delta.common.http.exceptions import EnactApiError

class DPSHelper:
    def __init__(self, username: str, public_api_key: str):
        self.api_helper = APIHelper(username, public_api_key)
        self._multi_series_subscriptions: list[tuple[str, Callable, bool]] = []
        self._single_series_subscriptions: list[tuple[str, str]] = []
        self._suppress_restart = False
        self.__initialise()
        self.start_connection_monitor()
        self.last_updated_header = "DateTimeLastUpdated"

    def __initialise(self):
        self.enact_credentials = self.api_helper.credentials_holder
        self.data_by_subscription_id: dict[str, tuple[Callable[[str], None], pd.DataFrame, bool]] = {}
        access_token_factory = partial(self._fetch_bearer_token)
        self.hub_connection = (
            HubConnectionBuilder()
            .with_url(
                ep.DPS,
                 options={"access_token_factory": access_token_factory},
            )
            .build()
        )

        self.hub_connection.on_open(self._on_open)
        self.hub_connection.on_close((self._on_close))
        self.hub_connection.on_error(lambda e: print("SignalR error:", e))
        self.hub_connection.on_reconnect(lambda: print("reconnected!"))

        success = self.hub_connection.start()
        pytime.sleep(1)

        if not success:
            raise ValueError("connection failed")

    def _fetch_bearer_token(self):
        self.enact_credentials.get_bearer_token()
        return self.enact_credentials.bearer_token

    def _add_subscription(self, request_object: list[dict[str, str]], subscription_id: str):
        self.hub_connection.send(
            "JoinEnactPush", request_object, lambda m: self._callback_received(m.result, subscription_id)
        )

    def _on_open(self):
        print("connection opened and handshake received ready to send messages")
        # On first connection single and multi series subscriptions are empty so nothing is done, but on reconecting this will open up the old pushes to not increase API usage again
        for push_group_name, subscription_id in self._single_series_subscriptions:
            try:
                self.hub_connection.send(
                    "ReconnectToPush", [push_group_name], lambda m: self._callback_received(m.result, subscription_id, is_for_reconnect=True)
                )
            except Exception as e:
                print(f"Resubscribe failed: {e}")

        for push_group_name, handler, parse_datetimes in self._multi_series_subscriptions:
            try:
                self.hub_connection.send(
                    "ReconnectToPush",
                    [push_group_name],
                    lambda m: self._callback_received_multi_series(m.result, handler, parse_datetimes, is_for_reconnect=True),
                )
            except Exception as e:
                print(f"Resubscribe failed: {e}")

    def _on_close(self):
        print("Connection closed")
        if self._suppress_restart:
            return

        if not self.hub_connection.transport.is_running():
            print(f"Attempting to restart hub connection")
            self._restart_connection()

    def _restart_connection(self):
        try:
            self.hub_connection.stop()
        except Exception as e:
            print(f"Error during stop: {e}")

        pytime.sleep(1)
        self.__initialise()

    def is_connection_alive(self):
        return self.hub_connection and self.hub_connection.transport and self.hub_connection.transport.is_running()

    def start_connection_monitor(self, check_interval_seconds=60):
        def monitor_loop():
            while True:
                pytime.sleep(check_interval_seconds)
                try:
                    if not self.is_connection_alive():
                        print("Connection not alive. Restarting...")
                        self._restart_connection()
                except Exception as e:
                    print(f"Error during connection check: {e}")
        threading.Thread(target=monitor_loop, daemon=True).start()

    def _add_multi_series_subscription(self, request_object: list, handle_data_method, parse_datetimes):
        self.hub_connection.send(
            "JoinMultiSeries",
            request_object,
            lambda m: self._callback_received_multi_series(m.result, handle_data_method, parse_datetimes),
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
        self.data_by_subscription_id[self._get_subscription_id(series_id, country_id, option_id)] = (
            handle_data_method,
            initial_series_data,
            parse_datetimes,
        )

    def _callback_received(self, m, subscription_id: str, is_for_reconnect = False):
        push_name = m["data"]["pushName"]
        if not is_for_reconnect:
            self._single_series_subscriptions.append((push_name, subscription_id))
        self.hub_connection.on(m["data"]["pushName"], lambda x: self._process_push_data(x, subscription_id))

    def _callback_received_multi_series(self, m, handle_data_method, parse_datetimes, is_for_reconnect = False):
        if 'messages' in m and len(m['messages']) > 0:
            error_return = m['messages'][0]
            raise EnactApiError(error_return["errorCode"], error_return["message"], m)

        push_name = m["data"]["pushName"]
        if not is_for_reconnect:
            self._multi_series_subscriptions.append((push_name, handle_data_method, parse_datetimes))
        self.hub_connection.on(push_name, lambda x: self._process_multi_series_push(x, handle_data_method, parse_datetimes))

    def _process_push_data(self, data_push, subscription_id):
        (user_callback, all_data, parse_datetimes) = self.data_by_subscription_id[subscription_id]
        updated_data = self._handle_new_series_data(all_data, data_push, parse_datetimes)
        user_callback(updated_data)

    def _process_multi_series_push(self, data_push, handle_data_method, parse_datetimes):
        updated_data = self._handle_new_mutli_series_data(data_push, parse_datetimes)
        handle_data_method(updated_data)

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

    def _handle_new_mutli_series_data(
        self, data_push_holder: list, parse_datetimes: bool
    ) -> pd.DataFrame:
        try:
            data_push = data_push_holder[0]["data"]
            pushes = data_push["data"]
            series_id = data_push['id']
            df_return = pd.DataFrame()
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
                for push_value in push_values:
                    df_return.loc[push_date_time, series_id] = push_value
                    df_return.loc[push_date_time, self.last_updated_header] = dt.now()

            return df_return
        except Exception:
            return data_push_holder

    def terminate_hub_connection(self):
        self._suppress_restart = True
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
        subscription_id = self._get_subscription_id(series_id, country_id, option_id)
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

    def subscribe_to_multiple_series_updates(
        self,
        handle_data_method: Callable[[str], None],
        series_requests: list[dict],
        parse_datetimes: bool = False,
    ) -> None:
        """
        Subscribe to multiple series at once with the specified series IDs, option IDs (if applicable) and country ID.

        Args:
            handle_data_method `Callable`: A callback function that will be invoked when any of the series are updated.
                The function should accept one argument, which will be the data received from the series updates.

            series_requests `list[dict]`: A list of dictionaries, with each element detailing a series request. Each element needs a countryId, seriesId, and if relevant, optionIds.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.


        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.
        """
        join_payload = []
        for series_entry in series_requests:
            series_id = series_entry["seriesId"]
            if not isinstance(series_id, str):
                    raise ValueError("Please ensure that all series ids are string types.")

            series_payload = {"seriesId": series_id}

            if "countryId" in series_entry:
                countryId = series_entry["countryId"]
                series_payload["countryId"] = countryId

            if "optionIds" in series_entry:
                option_ids = series_entry["optionIds"]
                if not is_2d_list_of_strings(option_ids):
                    raise ValueError(
                        f"Series options incorrectly formatted for series {series_id}. Please use a 2-Dimensional list of string values, or `None` for series without options."
                    )
                series_payload["optionIds"] = option_ids

            join_payload.append(series_payload)

        self._add_multi_series_subscription([join_payload], handle_data_method, parse_datetimes)

    def _get_subscription_id(self, series_id: str, country_id: str, option_id: list[str]) -> tuple:
        subscription_id = (series_id, country_id)
        if option_id:
            subscription_id += tuple(option_id)
        return subscription_id

    def _extract_subscription_id_from_data_push(self, data_push):
        parts = data_push[0]["data"]["id"].split("&")
        series_id = parts[1]
        country_id = parts[0]
        options = parts[2:]  # potentially empty

        if len(options) == 1 and options[0] == "none":
            return (series_id, country_id, None)

        return (series_id, country_id, *options)