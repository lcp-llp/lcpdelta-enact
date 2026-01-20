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
from pysignalr.client import SignalRClient
from lcp_delta.global_helpers import is_list_of_strings_or_empty, is_2d_list_of_strings
from lcp_delta.enact.api_helper import APIHelper
from lcp_delta.common.http.exceptions import EnactApiError


class DPSHelperAsyncV4:
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

        self._connected = asyncio.Event()
        self._initialise()

    

    def _initialise(self):
        self.enact_credentials = self.api_helper.credentials_holder
        self.data_by_single_series_subscription_id: dict[object, tuple[Callable[[pd.DataFrame], None], pd.DataFrame, bool]] = {}
        access_token_factory = partial(self._fetch_bearer_token)
        base_url = "https://localhost:44330/dataHub"
        self.hub_connection = SignalRClient(
            base_url,
            access_token_factory=access_token_factory,
            headers={"Authorization": f"Bearer {access_token_factory()}"}
        )

        self.hub_connection.on_open(self._on_open)
        self.hub_connection.on_close(self._on_close)

    async def run(self):
        await self.hub_connection.run()

    def _fetch_bearer_token(self):
        self.enact_credentials.get_bearer_token()
        return self.enact_credentials.bearer_token

    
    async def _add_subscription(self, request_object: list[dict[str, str]], subscription_id: str):
        print("[DEBUG] - _add_subscription called")

        async def on_join_result(m):
            print("[DEBUG] - on_join_result called")
            result = m.result
            await self._callback_received(result, subscription_id)

        await self.hub_connection.send(
            "JoinEnactPush", 
            request_object,
            on_join_result
            )


    async def _on_open(self):
        self._connected.set()
        print("[DEBUG] - _on_open called")


    async def _on_close(self):
        print("Connection closed")

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

    async def _callback_received(self, m, subscription_id: str, is_for_reconnect: bool = False):
        print("[DEBUG] - _callback_received called")
        push_name = m["data"]["pushName"]
        if not is_for_reconnect:
            self._single_series_subscriptions.append((push_name, subscription_id))

        async def push_handler(x):
            await self._process_push_data(x, subscription_id)
        
        self.hub_connection.on(push_name, push_handler)

    async def _process_push_data(self, data_push, subscription_id):
        print("[DEBUG] - _process_push_data called")

        (user_callback, all_data, parse_datetimes) = self.data_by_single_series_subscription_id[subscription_id]

        try:
            series_id = data_push[0]["data"]["id"]
        except Exception:
            series_id = str(subscription_id)

        async def work(data):
            updated_data = self._handle_new_series_data(data, data_push, parse_datetimes)
            if updated_data is not data:
                self.data_by_single_series_subscription_id[subscription_id] = (
                    user_callback,
                    updated_data,
                    parse_datetimes,
                )

            self._invoke_handler(user_callback, updated_data)

        self._enqueue_or_call(series_id, work, all_data)


    def _enqueue_or_call(self, series_id: str, handler: Callable, data):
        """
        In blocking mode: call directly.
        In async mode: enqueue (series-ordered) and schedule via shared thread pool respecting max_workers.
        """
        print("[DEBUG] - _enqueue_or_call called")
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
        print("[DEBUG] - _invoke_handler called")
        asyncio.create_task(handler(data))
        # if inspect.iscoroutinefunction(handler):
        #     try:
        #         loop = asyncio.get_running_loop()
        #         future = asyncio.run_coroutine_threadsafe(handler(data), loop)
        #         future.result()
        #     except RuntimeError:
        #         asyncio.create_task(handler(data))
        # else:
        #     handler(data)

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


    async def subscribe_to_series_updates(
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
        await self._connected.wait()
        print("[DEBUG] - subscribe_to_series_updates called")
    
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

        await self._add_subscription(enact_request_object_series, subscription_id)

    
    def _get_subscription_id(self, series_id: str, country_id: str, option_id: list[str]) -> tuple:
        subscription_id = (series_id, country_id)
        if option_id:
            subscription_id += tuple(option_id)
        return subscription_id
    


if __name__ == "__main__":

    async def main():
        username = "LcpInternalEnactAccessBaileyHalliday"
        api_key = "28AACrbX79aH"

        dps_helper_async = DPSHelperAsyncV4(username, api_key)

        async def handle_updates(df):
            print(df)

        await asyncio.gather(
            dps_helper_async.run(),
            dps_helper_async.subscribe_to_series_updates(handle_updates,"ImbalancePriceRealtime", country_id="Belgium", parse_datetimes=True)
    )

    asyncio.run(main())
    