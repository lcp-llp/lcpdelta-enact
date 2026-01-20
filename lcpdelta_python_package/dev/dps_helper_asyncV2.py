import logging
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
from pysignalr.client import SignalRClient
import urllib.parse


class DPSHelperAsyncV2:
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

        self.enact_credentials = self.api_helper.credentials_holder
        self.data_by_single_series_subscription_id: dict[object, tuple[Callable[[pd.DataFrame], None], pd.DataFrame, bool]] = {}

    async def start(self):

        # Create client      
        access_token_factory = partial(self._fetch_bearer_token)
        base_url = "https://localhost:44330/dataHub"
        # base_url = self.api_helper.endpoints.DPS
        # url_with_token = base_url + f"?access_token={self._fetch_bearer_token()}"
        client = SignalRClient(
            base_url,
            access_token_factory=access_token_factory,
            headers={"Authorization": f"Bearer {self._fetch_bearer_token()}"}
        )
        

        # Register callbacks with debugging
        client.on_open(self.on_open)
        client.on_close(self.on_close)
        client.on_error(self.on_error)

        print("[PYTHON] Starting SignalR client...")
        await client.run()


    async def on_open(self):
        print("[DEBUG] on_open called")

    async def on_close(self):
        print("[DEBUG] on_close called")

    async def on_error(self, e):
        print("[DEBUG] SignalR error:", e)




    # def _initialise(self):
    #     self.enact_credentials = self.api_helper.credentials_holder
    #     self.data_by_single_series_subscription_id: dict[object, tuple[Callable[[pd.DataFrame], None], pd.DataFrame, bool]] = {}
    #     access_token_factory = partial(self._fetch_bearer_token)
    #     self.hub_connection = (
    #         HubConnectionBuilder()
    #         .with_url(
    #             self.api_helper.endpoints.DPS,
    #             options={"access_token_factory": access_token_factory},
    #         )
    #         .build()
    #     )

    #     self.hub_connection.on_open(lambda: print("on_open called!"))
    #     self.hub_connection.on_close(lambda: print("on_close called!"))
    #     self.hub_connection.on_error(lambda e: print("SignalR error:", e))

    #     success = self.hub_connection.start()
    #     pytime.sleep(1)

    #     if not success:
    #         raise ValueError("connection failed")
        
    def _fetch_bearer_token(self):
        self.enact_credentials.get_bearer_token()
        token = self.enact_credentials.bearer_token
        print(f"[PYTHON] Token factory called, returning: {token[:5]}...")
        return self.enact_credentials.bearer_token
    


