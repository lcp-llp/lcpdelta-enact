from collections.abc import Awaitable
import logging
import pandas as pd
import threading
import inspect
import asyncio
import zoneinfo
from datetime import datetime as dt
from datetime import timezone, timedelta
from functools import partial
from typing import Any, Callable
from pysignalr.client import SignalRClient
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
        # suppress logging pushes with no registered handlers
        logging.getLogger("pysignalr").setLevel(logging.ERROR) 

        self.api_helper = APIHelper(username, public_api_key)
        self._multi_series_subscriptions: list[tuple[str, Callable, bool]] = []
        self._single_series_subscriptions: list[tuple[str, str]] = []
        self._connection_open = False
        self.last_updated_header = "DateTimeLastUpdated"

        self.async_mode = async_mode
        self.max_workers = max_workers

        if self.async_mode:
            self._series_queues: dict[str, asyncio.Queue] = {}
            self._series_semaphore = asyncio.Semaphore(max_workers)
            self._series_lock = threading.Lock()
            self._series_running: set[str] = set()

        # Background event loop for SignalR
        self._loop = None     
        self._loop_ready = threading.Event()
        self._client_initialised = threading.Event()


        # Start event loop on background thread
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()

        # init SignalR client in background loop
        self._loop_ready.wait()
        self._run_in_loop(self._initialise())


    # Background event loop helpers
    def _start_loop(self):
        """Run a dedicated asyncio event loop in a background thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)  
        self._loop_ready.set()     
        self._loop.run_forever()
            
    def _run_in_loop(self, coro):
        """Submit a coroutine to the background loop from any thread."""
        if not self._loop:
            raise RuntimeError("Event loop not ready")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)
    
    def _ensure_async(self, func: Callable[..., Any]) -> Callable[..., Awaitable[Any]]:
        if inspect.iscoroutinefunction(func):
            return func

        async def async_wrapper(*args, **kwargs):
            return await asyncio.to_thread(func, *args, **kwargs)
        return async_wrapper
    
    
    # SignalR client initialization
    async def _initialise(self):
        self.enact_credentials = self.api_helper.credentials_holder
        self.data_by_single_series_subscription_id: dict[
            object, tuple[Callable[[pd.DataFrame], None], pd.DataFrame, bool]
        ] = {}

        access_token_factory = partial(self._fetch_bearer_token)
        url = self.api_helper.endpoints.DPS

        self.hub_connection = SignalRClient(
            url,
            access_token_factory=access_token_factory,
            headers={"Authorization": f"Bearer {access_token_factory()}"},
        )

        self.hub_connection.on_open(self._on_open)
        self.hub_connection.on_close(self._on_close)
        self.hub_connection.on_error(lambda e: print(f"SignalR error: {e}"))

        # Start SignalR client
        self._signalr_task = asyncio.create_task(self.hub_connection.run())
        self._client_initialised.set()

    def _fetch_bearer_token(self):
        self.enact_credentials.get_bearer_token()
        return self.enact_credentials.bearer_token
     
    async def _add_subscription(self, request_object: list[dict[str, str]], subscription_id: str):
        # Create wrapper to capture subscription_id in callback
        async def on_JoinEnactPush(m):
            result = m.result
            await self._callback_received(result, subscription_id)

        await self.hub_connection.send(
            "JoinEnactPush", 
            request_object,
            on_JoinEnactPush
            )

    async def _on_open(self):
        print("Connection opened and handshake received ready to send messages")
        # Reconnect to prior pushes without increasing API usage
        for push_group_name, _ in self._single_series_subscriptions:
            try:
                await self.hub_connection.send(
                    "ReconnectToPush",
                    [push_group_name],
                )
            except Exception as e:
                print(f"Resubscribe failed (single): {e}")

        for push_group_name, _, _ in self._multi_series_subscriptions:
            try:
                await self.hub_connection.send(
                    "ReconnectToPush",
                    [push_group_name],
                )
            except Exception as e:
                print(f"Resubscribe failed (multi): {e}")

        self._connection_open = True

    async def _on_close(self):
        if(self._connection_open == False):
            return       
        
        print("Connection closed")
        self._connection_open = False

    async def _add_multi_series_subscription(self, request_object: list, handle_data_method, parse_datetimes):
        # Create wrapper to capture handle_data_method, parse_datetimes in callback
        async def on_JoinMultiSeries(m):
            result = m.result
            await self._callback_received_multi_series(result, handle_data_method, parse_datetimes)

        await self.hub_connection.send(
            "JoinMultiSeries",
            request_object,
            on_JoinMultiSeries,
        )

    def subscribe_to_notifications(self, handle_notification_method: Callable[[object], None]):
        self._client_initialised.wait()
        handle_notification_method = self._ensure_async(handle_notification_method)
        self._run_in_loop(
            self._add_notification_subscription(handle_notification_method)
        )

    async def _add_notification_subscription(self, handle_notification_method):
        async def on_join_parent_company_notification_push(m):         
            push_name = m.result["data"]["pushName"]
            self.hub_connection.on(push_name, handle_notification_method)

        await self.hub_connection.send(
            "JoinParentCompanyNotificationPush",
            [],
            on_join_parent_company_notification_push,
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

    async def _callback_received(self, m, subscription_id: str, is_for_reconnect: bool = False):
        push_name = m["data"]["pushName"]
        if not is_for_reconnect:
            self._single_series_subscriptions.append((push_name, subscription_id))

        # Create wrapper to capture subscription_id in callback
        async def push_handler(x):
            await self._process_push_data(x, subscription_id)
        
        self.hub_connection.on(push_name, push_handler)

    async def _callback_received_multi_series(self, m, handle_data_method, parse_datetimes, is_for_reconnect: bool = False):
        if "messages" in m and len(m["messages"]) > 0:
            error_return = m["messages"][0]
            raise EnactApiError(error_return["errorCode"], error_return["message"], m)

        push_name = m["data"]["pushName"]
        if not is_for_reconnect:
            self._multi_series_subscriptions.append((push_name, handle_data_method, parse_datetimes))

        async def push_handler(x):
            await self._process_multi_series_push(x, handle_data_method, parse_datetimes)

        self.hub_connection.on(push_name, push_handler)

    async def _process_push_data(self, data_push, subscription_id):
        if subscription_id == EPEX_SUBSCRIPTION_ID:
            await self._enqueue_or_call(subscription_id, self.epex_trade_call_back, data_push)
            return

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

            await user_callback(updated_data)

        await self._enqueue_or_call(series_id, work, all_data)

    async def _process_multi_series_push(self, data_push, handle_data_method, parse_datetimes):
        updated_data = self._handle_new_multi_series_data(data_push, parse_datetimes)
        series_id = data_push[0]["data"]["id"]
        await self._enqueue_or_call(series_id, handle_data_method, updated_data)

    async def _enqueue_or_call(self, series_id: str, handler: Callable, data):
        """
        In blocking mode: call directly.
        In async mode: enqueue (series-ordered) and schedule via shared event loop with max_workers limit.
        """
        if not self.async_mode:
            await handler(data)
            return

        first = False
        with self._series_lock:
            q = self._series_queues.get(series_id)
            if q is None:
                q = self._series_queues[series_id] = asyncio.Queue()
            q.put_nowait((handler, data))
            if series_id not in self._series_running:
                self._series_running.add(series_id)
                first = True

        if first:
            asyncio.create_task(self._drain_one(series_id))

    async def _drain_one(self, series_id: str):
        """Process all items from the series queue sequentially, respecting max_workers limit."""
        async with self._series_semaphore:
            while True:
                with self._series_lock:
                    q = self._series_queues.get(series_id)
                    if q is None or q.empty():
                        self._series_running.discard(series_id)
                        return
                    handler, data = q.get_nowait()

                try:
                    await handler(data)
                except Exception as e:
                    print(f"Error in callback for series {series_id}: {e}")

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
        
    
    async def _cancel_signalr_task(self):
        """Cancel the SignalR task gracefully"""
        if hasattr(self, '_signalr_task') and not self._signalr_task.done():
            self._signalr_task.cancel()
            try:
                await self._signalr_task
            except asyncio.CancelledError:
                pass # SignalR task cancelled successfully            
            except Exception as e:
                print(f"Error during task cancellation: {e}")

    async def _shutdown_async(self):
        """Async shutdown helper to cancel all tasks"""     

        # Clear all drain state to prevent semaphore deadlocks
        if self.async_mode:
            with self._series_lock:
                self._series_running.clear()

        await self._cancel_signalr_task()
        await asyncio.sleep(0.5)
        
        # Cancel all remaining tasks in this event loop except the current one
        current_task = asyncio.current_task(self._loop)
        tasks = [
            t for t in asyncio.all_tasks(self._loop) 
            if not t.done() and t is not current_task  
        ]
        
        for task in tasks:
            task.cancel()
        
        if tasks: 
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Final cleanup
        if self.async_mode:
            with self._series_lock:
                self._series_queues.clear()
            
    def terminate_hub_connection(self):
        print("Shutting down DPSHelper...")

        # Shutdown async components
        if self._loop and self._loop.is_running():
            future = self._run_in_loop(self._shutdown_async())
            try:
                future.result(timeout=10)
            except Exception as ex:
                print(f"Error during async shutdown: {ex}")

        # Stop the background event loop
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        
        # Wait for background thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                print("Warning: Background thread did not stop cleanly")
        
        print("DPSHelper shutdown complete")

    def subscribe_to_epex_trade_updates(self, handle_data_method: Callable[[object], None]) -> None:
        """
        `THIS FUNCTION IS IN BETA`
        Subscribe to EPEX trade updates and specify a callback function to handle the received data.

        Args:
            handle_data_method `Callable`: A callback function that will be invoked with the received EPEX trade updates.
                The function should accept one argument, which will be the data received from the EPEX trade updates.
        """
        handle_data_method = self._ensure_async(handle_data_method)
        self._client_initialised.wait()

        enact_request_object_epex = [{"Type": "EPEX", "Group": "Trades"}]

        self.epex_trade_call_back = handle_data_method

        self._run_in_loop(
            self._add_subscription(enact_request_object_epex, EPEX_SUBSCRIPTION_ID)
        )

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
        self._client_initialised.wait()
        handle_data_method = self._ensure_async(handle_data_method)
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

        self._run_in_loop(
            self._add_subscription(enact_request_object_series, subscription_id)
        )

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
        self._client_initialised.wait()
        handle_data_method = self._ensure_async(handle_data_method)
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

        self._run_in_loop(
            self._add_multi_series_subscription([join_payload], handle_data_method, parse_datetimes)
        )

    def _get_subscription_id(self, series_id: str, country_id: str, option_id: list[str]) -> tuple:
        subscription_id = (series_id, country_id)
        if option_id:
            subscription_id += tuple(option_id)
        return subscription_id
    