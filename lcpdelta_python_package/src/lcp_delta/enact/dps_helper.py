import time as pytime
import pandas as pd
import threading
import queue
import inspect
import asyncio
import zoneinfo
from datetime import datetime as dt
from datetime import timezone, time, timedelta
from functools import partial
from typing import Callable
from concurrent.futures import ThreadPoolExecutor
from signalrcore.hub_connection_builder import HubConnectionBuilder
from lcp_delta.global_helpers import is_list_of_strings_or_empty, is_2d_list_of_strings
from lcp_delta.enact.api_helper import APIHelper
from lcp_delta.common.http.exceptions import EnactApiError

EPEX_SUBSCRIPTION_ID = "EPEX_TRADES"

class DPSHelper:
    def __init__(
        self,
        username: str,
        public_api_key: str,
        async_mode: bool = False,
        max_workers: int = 1,
    ):
        """
        async_mode:
            False (default) -> blocking behaviour: callbacks run synchronously
            True             -> callbacks run via a per-series FIFO queue with parallelism across series
        max_workers:
            Maximum number of callback tasks that can run at once (only used when async_mode=True)
        """
        self.api_helper = APIHelper(username, public_api_key)
        self._multi_series_subscriptions: list[tuple[str, Callable, bool]] = []
        self._single_series_subscriptions: list[tuple[str, str]] = []
        self._suppress_restart = False
        self.last_updated_header = "DateTimeLastUpdated"

        self.async_mode = async_mode
        self.max_workers = max_workers
        if self.async_mode:
            self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dps-callback")
            self._series_queues: dict[str, queue.Queue] = {}
            self._series_running: set[str] = set()
            self._series_lock = threading.Lock()

        self._initialise()
        self.start_connection_monitor()

    def _initialise(self):
        self.enact_credentials = self.api_helper.credentials_holder
        self.data_by_single_series_subscription_id: dict[object, tuple[Callable[[pd.DataFrame], None], pd.DataFrame, bool]] = {}
        access_token_factory = partial(self._fetch_bearer_token)
        self.hub_connection = (
            HubConnectionBuilder()
            .with_url(
                self.api_helper.endpoints.DPS,
                 options={"access_token_factory": access_token_factory},
            )
            .build()
        )

        self.hub_connection.on_open(self._on_open)
        self.hub_connection.on_close(self._on_close)
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
        # Reconnect to prior pushes without increasing API usage
        for push_group_name, subscription_id in self._single_series_subscriptions:
            try:
                self.hub_connection.send(
                    "ReconnectToPush",
                    [push_group_name],
                    lambda m, sid=subscription_id: self._callback_received(m.result, sid, is_for_reconnect=True)
                )
            except Exception as e:
                print(f"Resubscribe failed (single): {e}")

        for push_group_name, handler, parse_datetimes in self._multi_series_subscriptions:
            try:
                self.hub_connection.send(
                    "ReconnectToPush",
                    [push_group_name],
                    lambda m, h=handler, pdts=parse_datetimes: self._callback_received_multi_series(m.result, h, pdts, is_for_reconnect=True),
                )
            except Exception as e:
                print(f"Resubscribe failed (multi): {e}")

    def _on_close(self):
        print("Connection closed")
        if self._suppress_restart:
            return

        if not self.hub_connection.transport.is_running():
            print("Attempting to restart hub connection")
            self._restart_connection()

    def _restart_connection(self):
        try:
            self.hub_connection.stop()
        except Exception as e:
            print(f"Error during stop: {e}")

        pytime.sleep(1)
        self._initialise()

    def is_connection_alive(self):
        return self.hub_connection and self.hub_connection.transport and self.hub_connection.transport.is_running()

    def start_connection_monitor(self, check_interval_seconds=60):
        if hasattr(self, "_monitor_thread") and self._monitor_thread.is_alive():
            # Monitor is already running
            return

        self._stop_event = getattr(self, "_stop_event", threading.Event())

        def monitor_loop():
            backoff = 1
            while not self._stop_event.wait(check_interval_seconds * backoff):
                try:
                    if not self.is_connection_alive():
                        print("Connection not alive. Restarting...")
                        self._restart_connection()
                        backoff = min(backoff * 2, 8)
                    else:
                        backoff = 1
                except Exception as e:
                    print(f"Error during connection check: {e}")

        self._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self._monitor_thread.start()

    def _add_multi_series_subscription(self, request_object: list, handle_data_method, parse_datetimes):
        self.hub_connection.send(
            "JoinMultiSeries",
            request_object,
            lambda m: self._callback_received_multi_series(m.result, handle_data_method, parse_datetimes),
        )

    def subscribe_to_notifications(self, handle_notification_method: Callable[[object], None]):
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
        handle_data_method: Callable[[pd.DataFrame], None],
        parse_datetimes: bool,
    ):
        system_local = dt.now().astimezone()
        gb_tz = zoneinfo.ZoneInfo("Europe/London")
        start_local_midnight = system_local.replace(hour=0, minute=0, second=0, microsecond=0)
        start_gb = start_local_midnight.astimezone(gb_tz)
        end_local_midnight = start_local_midnight + timedelta(days=1)
        end_gb = end_local_midnight.astimezone(gb_tz)

        initial_series_data = self.api_helper.get_series_data(
            series_id, start_gb, end_gb, country_id, option_id, parse_datetimes=parse_datetimes,  request_time_zone_id="GMT Standard Time"
        )
        initial_series_data[self.last_updated_header] = dt.now(timezone.utc)
        self.data_by_single_series_subscription_id[self._get_subscription_id(series_id, country_id, option_id)] = (
            handle_data_method,
            initial_series_data,
            parse_datetimes,
        )

    def _callback_received(self, m, subscription_id: str, is_for_reconnect: bool = False):
        push_name = m["data"]["pushName"]
        if not is_for_reconnect:
            self._single_series_subscriptions.append((push_name, subscription_id))
        self.hub_connection.on(push_name, lambda x: self._process_push_data(x, subscription_id))

    def _callback_received_multi_series(self, m, handle_data_method, parse_datetimes, is_for_reconnect: bool = False):
        if "messages" in m and len(m["messages"]) > 0:
            error_return = m["messages"][0]
            raise EnactApiError(error_return["errorCode"], error_return["message"], m)

        push_name = m["data"]["pushName"]
        if not is_for_reconnect:
            self._multi_series_subscriptions.append((push_name, handle_data_method, parse_datetimes))
        self.hub_connection.on(push_name, lambda x: self._process_multi_series_push(x, handle_data_method, parse_datetimes))

    def _process_push_data(self, data_push, subscription_id):

        if subscription_id == EPEX_SUBSCRIPTION_ID:
            self._enqueue_or_call(subscription_id, self.epex_trade_call_back, data_push)
            return

        (user_callback, all_data, parse_datetimes) = self.data_by_single_series_subscription_id[subscription_id]

        try:
            series_id = data_push[0]["data"]["id"]
        except Exception:
            series_id = str(subscription_id)

        def work(data):
            updated_data = self._handle_new_series_data(data, data_push, parse_datetimes)
            if updated_data is not data:
                self.data_by_single_series_subscription_id[subscription_id] = (
                    user_callback,
                    updated_data,
                    parse_datetimes,
                )

            self._invoke_handler(user_callback, updated_data)

        self._enqueue_or_call(series_id, work, all_data)

    def _process_multi_series_push(self, data_push, handle_data_method, parse_datetimes):
        updated_data = self._handle_new_multi_series_data(data_push, parse_datetimes)
        series_id = data_push[0]["data"]["id"]
        self._enqueue_or_call(series_id, handle_data_method, updated_data)

    def _enqueue_or_call(self, series_id: str, handler: Callable, data):
        """
        In blocking mode: call directly.
        In async mode: enqueue (series-ordered) and schedule via shared thread pool respecting max_workers.
        """
        if not self.async_mode:
            self._invoke_handler(handler, data)
            return

        first = False
        with self._series_lock:
            q = self._series_queues.get(series_id)
            if q is None:
                q = self._series_queues[series_id] = queue.Queue()
            q.put((handler, data))
            if series_id not in self._series_running:
                self._series_running.add(series_id)
                first = True  # we need to start the drain

        if first:
            self._executor.submit(self._drain_one, series_id)

    def _drain_one(self, series_id: str):
        """Process exactly one item from the series queue, then reschedule if needed."""
        with self._series_lock:
            q = self._series_queues.get(series_id)
            if q is None or q.empty():
                self._series_running.discard(series_id)
                return

            handler, data = q.get_nowait()

        try:
            # Run the callback safely
            self._invoke_handler(handler, data)
        except Exception as e:
            print(f"Error in callback for series {series_id}: {e}")
        finally:
            q.task_done()

        # Reschedule the next item if there’s still work to do
        with self._series_lock:
            if not q.empty():
                self._executor.submit(self._drain_one, series_id)
            else:
                self._series_running.discard(series_id)

    def _invoke_handler(self, handler: Callable, data):
        """
        Runs sync or async handlers appropriately.
        (Pool threads are not running an event loop, so we create one for async handlers.)
        """
        if inspect.iscoroutinefunction(handler):
            try:
                loop = asyncio.get_running_loop()
                future = asyncio.run_coroutine_threadsafe(handler(data), loop)
                future.result()
            except RuntimeError:
                asyncio.run(handler(data))
        else:
            handler(data)

    def _handle_new_series_data(
        self, all_data: pd.DataFrame, data_push_holder: list, parse_datetimes: bool
    ) -> pd.DataFrame:
        try:
            data_push = data_push_holder[0]["data"]
            push_ids = list(all_data.columns)[:-1] if not all_data.empty else []
            pushes = data_push["data"]
            for push in pushes:
                push_current = push["current"]
                push_date_time = f"{push_current['datePeriod']['datePeriodCombinedGmt']}"
                if not push_date_time.endswith("Z"):
                    push_date_time += "Z"

                if parse_datetimes:
                    push_date_time = pd.to_datetime(push_date_time, utc=True)

                push_values = (
                    push_current["arrayPoint"][1:]
                    if not push["byPoint"]
                    else list(push_current["objectPoint"].values())
                )

                for index, push_value in enumerate(push_values):
                    col_name = push_ids[index] if push_ids else index
                    if col_name not in all_data.columns:
                        all_data[col_name] = pd.NA
                    all_data.loc[push_date_time, col_name] = push_value

                if self.last_updated_header not in all_data.columns:
                    all_data[self.last_updated_header] = pd.NA
                all_data.loc[push_date_time, self.last_updated_header] = dt.now(timezone.utc)

            return all_data
        except Exception:
            return all_data

    def _handle_new_multi_series_data(
        self, data_push_holder: list, parse_datetimes: bool
    ) -> pd.DataFrame:
        try:
            data_push = data_push_holder[0]["data"]
            pushes = data_push["data"]
            series_id = data_push["id"]
            df_return = pd.DataFrame()
            for push in pushes:
                push_current = push["current"]
                push_date_time = f"{push_current['datePeriod']['datePeriodCombinedGmt']}"

                if not push_date_time.endswith("Z"):
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
                    df_return.loc[push_date_time, self.last_updated_header] = dt.now(timezone.utc)

            return df_return
        except Exception:
            return pd.DataFrame()

    def terminate_hub_connection(self):
        self.shutdown()

    def shutdown(self):
        self._suppress_restart = True
        self._stop_event.set()

        if self.hub_connection:
            try:
                self.hub_connection.stop()
            except Exception as ex:
                print(f"Error stopping hub connection: {ex}")

        if self.async_mode:
            self._executor.shutdown(wait=True)

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)

    def subscribe_to_epex_trade_updates(self, handle_data_method: Callable[[object], None]) -> None:
        """
        `THIS FUNCTION IS IN BETA`
        Subscribe to EPEX trade updates and specify a callback function to handle the received data.

        Args:
            handle_data_method `Callable`: A callback function that will be invoked with the received EPEX trade updates.
                The function should accept one argument, which will be the data received from the EPEX trade updates.
        """
        if hasattr(self, "epex_trade_call_back") and self.epex_trade_call_back:
            self.hub_connection.off(EPEX_SUBSCRIPTION_ID)

        enact_request_object_epex = [{"Type": "EPEX", "Group": "Trades"}]

        self.epex_trade_call_back = handle_data_method

        self._add_subscription(enact_request_object_epex, EPEX_SUBSCRIPTION_ID)

    def subscribe_to_series_updates(
        self,
        handle_data_method: Callable[[pd.DataFrame], None],
        series_id: str,
        option_id: list[str] = None,
        country_id="Gb",
        parse_datetimes: bool = False,
    ) -> None:
        """
        Subscribe to series updates with the specified SeriesId and optional parameters.

        Args:
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
        if subscription_id in self.data_by_single_series_subscription_id:
            return
        (handle_data_old, initial_data_from_series_api, parse_datetimes_old) = self.data_by_single_series_subscription_id.get(
            subscription_id, (None, pd.DataFrame(), False)
        )
        if initial_data_from_series_api.empty:
            self._initialise_series_subscription_data(
                series_id, country_id, option_id, handle_data_method, parse_datetimes
            )
        else:
            self.data_by_single_series_subscription_id[subscription_id] = (
                handle_data_method,
                initial_data_from_series_api,
                parse_datetimes_old,
            )

        enact_request_object_series = [request_details]
        self._add_subscription(enact_request_object_series, subscription_id)

    def subscribe_to_multiple_series_updates(
        self,
        handle_data_method: Callable[[pd.DataFrame], None],
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