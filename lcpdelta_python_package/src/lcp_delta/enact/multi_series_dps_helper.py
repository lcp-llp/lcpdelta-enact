from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import pandas as pd
from pysignalr.client import SignalRClient

from lcp_delta.enact.api_helper import APIHelper
from lcp_delta.common.http.exceptions import EnactApiError
from lcp_delta.global_helpers import is_2d_list_of_strings


ChartPushCallback = Callable[..., Any]
ReconnectCallback = Callable[..., Any]
MULTI_SERIES_RECONNECT_LEASE_DAYS = 14.0


class _SignalRMissingMethodFilter(logging.Filter):
    """Suppress pysignalr warnings for server methods this helper does not handle."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not (
            record.name == "pysignalr.client"
            and record.levelno == logging.WARNING
            and message.startswith("No client method with the name ")
            and message.endswith(" found.")
        )


_SIGNALR_MISSING_METHOD_FILTER = _SignalRMissingMethodFilter()
_SIGNALR_LOG_FILTER_LOCK = threading.Lock()


@dataclass(frozen=True)
class PlantPushMetadata:
    """Plant metadata attached by the SignalR hub when a plant series push is routed."""

    plant_id: str | None = None
    owner: str | None = None
    fuel: str | None = None
    zone: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class MultiSeriesPushMetadata:
    """Metadata describing a received multi-series chart push."""

    sequence: int
    received_at_utc: datetime
    group_name: str
    push_id: str | None
    series_id: str | None
    country_id: str | None
    option_ids: tuple[str, ...] = ()
    day: str | None = None
    replace_series: bool = False
    refresh: bool = False
    plant: PlantPushMetadata | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class _MultiSeriesSubscription:
    join_payload: list[dict[str, Any]]
    callback: ChartPushCallback
    parse_datetimes: bool


@dataclass
class _PushProcessingGate:
    ready: asyncio.Event
    drained: asyncio.Event
    pending: int = 0


class MultiSeriesDPSHelper:
    """Manage Enact multi-series chart pushes over SignalR.

    Callbacks receive a time-indexed dataframe plus a `MultiSeriesPushMetadata`
    object. Dataframes also carry the metadata in
    `df.attrs["enact_push_metadata"]` for one-argument callbacks.

    A single helper instance is intended to manage all multi-series chart push
    subscriptions for one client session. Reuse the same helper and call
    `close()` or use a context manager so the backend sees the SignalR
    disconnect promptly.
    """

    def __init__(
        self,
        username: str,
        public_api_key: str,
        callback: ChartPushCallback | None = None,
        reconnect_callback: ReconnectCallback | None = None,
        *,
        parse_datetimes: bool = True,
        concurrent_callbacks: bool = False,
        max_workers: int = 1,
        auto_connect: bool = True,
        reconnect: bool = True,
        reconnect_initial_delay_seconds: float = 2.0,
        reconnect_max_delay_seconds: float = 30.0,
        reconnect_callback_timeout_seconds: float = 60.0,
        lease_refresh_interval_days: float | None = 13.0,
        callback_queue_maxsize: int = 0,
        suppress_unhandled_server_method_logs: bool = True,
        logger: logging.Logger | None = None,
    ):
        """Create a multi-series DPS connection manager.

        Args:
            username: Enact username for API authentication.
            public_api_key: Public API key for the Enact user.
            callback: Optional default callback for chart pushes. It can accept
                `(dataframe, metadata)` or only `(dataframe)`. One-argument
                callbacks can read the same metadata from
                `dataframe.attrs["enact_push_metadata"]`.
            reconnect_callback: Optional callback run after reconnecting and
                restoring existing groups, after callbacks queued before the
                disconnect have drained and before reconnect-era pushes are
                processed. Use this to refresh local state from the API after
                a disconnect. It can be sync or async, and can accept either
                no arguments or this helper instance.
            parse_datetimes: When true, dataframe indexes are UTC pandas
                timestamps. When false, they are UTC ISO strings.
            concurrent_callbacks: When false, callbacks run one at a time and
                complete in receive order. When true, callback work is split
                across fixed worker shards. The same push stream always maps to
                the same shard, so it remains ordered while different streams
                can overlap, up to `max_workers`.
            max_workers: Maximum number of callback tasks allowed to run at the
                same time when `concurrent_callbacks=True`. Synchronous
                callbacks are executed in a bounded thread pool of this size.
                When `concurrent_callbacks=False`, callbacks are deliberately
                single-file and this value does not increase callback
                concurrency.
            auto_connect: When true, create the SignalR connection immediately.
                When false, construction performs no network work and the
                connection starts on `connect()` or first subscribe.
            reconnect: When true, supervise the SignalR connection and reconnect
                after an unexpected close. Existing groups use
                `ReconnectToPush` first, falling back to `JoinMultiSeries` only
                when the backend no longer accepts the previous group.
            reconnect_initial_delay_seconds: First wait before reconnecting.
            reconnect_max_delay_seconds: Maximum reconnect backoff delay.
            reconnect_callback_timeout_seconds: Maximum time spent retrying
                `reconnect_callback` after a reconnect before queued push
                processing is released anyway.
            lease_refresh_interval_days: How often to renew the backend
                multi-series group lease with `JoinMultiSeries`. The backend
                lease lasts 14 days. The default refreshes on day 13. Set to
                `None` to disable proactive lease refresh. This planned lease
                refresh is intentionally separate from disconnect recovery and
                may be counted by the backend like a fresh multi-series join.
            callback_queue_maxsize: Maximum number of received pushes queued or
                being processed across all shards. `0` means unbounded; set a
                finite value in long-running production clients if callbacks
                can be slower than the push rate.
            suppress_unhandled_server_method_logs: When true, suppress
                pysignalr warnings for backend broadcasts that do not have a
                matching client handler, such as unrelated push types this
                helper did not subscribe to. SignalR connection warnings and
                errors are still logged.
            logger: Optional logger for connection, reconnect and callback
                errors. Defaults to this module's logger.
        """
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        if reconnect_initial_delay_seconds < 0:
            raise ValueError("reconnect_initial_delay_seconds cannot be negative")
        if reconnect_max_delay_seconds < reconnect_initial_delay_seconds:
            raise ValueError(
                "reconnect_max_delay_seconds must be greater than or equal to reconnect_initial_delay_seconds"
            )
        if reconnect_callback_timeout_seconds < 0:
            raise ValueError("reconnect_callback_timeout_seconds cannot be negative")
        if (
            lease_refresh_interval_days is not None
            and not 0 < lease_refresh_interval_days < MULTI_SERIES_RECONNECT_LEASE_DAYS
        ):
            raise ValueError("lease_refresh_interval_days must be greater than 0 and less than 14")
        if callback_queue_maxsize < 0:
            raise ValueError("callback_queue_maxsize cannot be negative")
        if callback is not None and not callable(callback):
            raise TypeError("callback must be callable")
        if reconnect_callback is not None and not callable(reconnect_callback):
            raise TypeError("reconnect_callback must be callable")

        self._username = username
        self._public_api_key = public_api_key
        self.api_helper: APIHelper | None = None

        self.parse_datetimes = parse_datetimes
        self.concurrent_callbacks = concurrent_callbacks
        self.max_workers = max_workers
        self.reconnect = reconnect
        self.reconnect_initial_delay_seconds = reconnect_initial_delay_seconds
        self.reconnect_max_delay_seconds = reconnect_max_delay_seconds
        self.reconnect_callback_timeout_seconds = reconnect_callback_timeout_seconds
        self.lease_refresh_interval_days = lease_refresh_interval_days
        self.callback_queue_maxsize = callback_queue_maxsize
        self.suppress_unhandled_server_method_logs = suppress_unhandled_server_method_logs
        self.logger = logger or logging.getLogger(__name__)

        self._default_callback = callback
        self._reconnect_callback = reconnect_callback
        self._subscriptions: dict[str, _MultiSeriesSubscription] = {}
        self._subscription_group_by_request_key: dict[str, str] = {}
        self._handlers_registered_for_client: set[str] = set()
        self._sequence = 0

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_ready = threading.Event()
        self._client_initialised = threading.Event()
        self._connected = threading.Event()
        self._stop_requested = threading.Event()
        self._closed = False

        self.hub_connection: SignalRClient | None = None
        self._connection_task: asyncio.Task | None = None
        self._lease_refresh_task: asyncio.Task | None = None
        self._reconnect_tasks: set[asyncio.Task] = set()
        self._push_processing_gate: _PushProcessingGate | None = None
        self._pre_reconnect_drained: asyncio.Event | None = None
        self._callback_slots: asyncio.Semaphore | None = None
        self._callback_queues: list[asyncio.Queue] = []
        self._callback_tasks: list[asyncio.Task] = []
        self._callback_worker_count = max_workers if concurrent_callbacks else 1
        self._callback_executor: ThreadPoolExecutor | None = None

        if suppress_unhandled_server_method_logs:
            self._install_signalr_missing_method_filter()

        if auto_connect:
            self.connect()

    @property
    def is_connected(self) -> bool:
        """Return true when the current SignalR connection has completed its open handshake."""
        return self._connected.is_set()

    @property
    def group_names(self) -> tuple[str, ...]:
        """Return the backend multi-series group names currently managed by this helper."""
        return tuple(self._subscriptions.keys())

    def __enter__(self) -> "MultiSeriesDPSHelper":
        """Open the helper when used as a context manager."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        """Close the helper when leaving a context manager block."""
        self.close()

    def on_chart_push(self, callback: ChartPushCallback) -> ChartPushCallback:
        """Register the default chart push callback and return it for decorator use."""
        if not callable(callback):
            raise TypeError("callback must be callable")
        self._default_callback = callback
        return callback

    register_callback = on_chart_push

    def on_reconnected(self, callback: ReconnectCallback) -> ReconnectCallback:
        """Register a callback that runs before queued pushes resume after reconnect."""
        if not callable(callback):
            raise TypeError("callback must be callable")
        self._reconnect_callback = callback
        return callback

    def connect(self, timeout: float = 10) -> "MultiSeriesDPSHelper":
        """Start the background SignalR connection if it is not already running.

        This method is idempotent while the helper is active, so repeated calls
        do not create additional hub connections.
        """
        if self._closed:
            raise RuntimeError("This MultiSeriesDPSHelper has been closed. Create a new helper to reconnect.")

        if self._connection_task is not None and not self._connection_task.done():
            if not self._client_initialised.wait(timeout=timeout):
                raise TimeoutError("Timed out initialising the SignalR client")
            return self

        if self._loop is None or not self._loop.is_running():
            self._start_loop_thread()

        self._stop_requested.clear()
        self._client_initialised.clear()
        future = self._run_in_loop(self._ensure_connection_started())
        future.result(timeout=timeout)

        if not self._client_initialised.wait(timeout=timeout):
            raise TimeoutError("Timed out initialising the SignalR client")

        return self

    async def async_connect(self, timeout: float = 10) -> "MultiSeriesDPSHelper":
        """Start the SignalR connection without blocking the caller's event loop."""
        if self._closed:
            raise RuntimeError("This MultiSeriesDPSHelper has been closed. Create a new helper to reconnect.")

        if self._connection_task is not None and not self._connection_task.done():
            initialised = await asyncio.to_thread(self._client_initialised.wait, timeout)
            if not initialised:
                raise TimeoutError("Timed out initialising the SignalR client")
            return self

        if self._loop is None or not self._loop.is_running():
            await asyncio.to_thread(self._start_loop_thread)

        self._stop_requested.clear()
        self._client_initialised.clear()
        await asyncio.wait_for(asyncio.wrap_future(self._run_in_loop(self._ensure_connection_started())), timeout)
        initialised = await asyncio.to_thread(self._client_initialised.wait, timeout)
        if not initialised:
            raise TimeoutError("Timed out initialising the SignalR client")

        return self

    def subscribe_to_chart_pushes(
        self,
        series_requests: list[dict[str, Any]],
        callback: ChartPushCallback | None = None,
        *,
        parse_datetimes: bool | None = None,
        timeout: float = 30,
    ) -> str:
        """Subscribe to Enact multi-series chart pushes.

        `series_requests` uses the backend JoinMultiSeries shape:
        `[{"seriesId": "...", "countryId": "Gb", "optionIds": [["..."]]}]`.

        Args:
            series_requests: Non-empty list of multi-series requests. Each item
                requires `seriesId`, can include `countryId`, and can include
                `optionIds` as a two-dimensional list of strings.
            callback: Optional callback for this subscription. Use this when
                different subscriptions need different handling. If omitted,
                the default callback supplied at construction or via
                `on_chart_push()` is used.
            parse_datetimes: Optional per-subscription override for dataframe
                index parsing.
            timeout: Seconds to wait for the SignalR connection and
                `JoinMultiSeries` response.

        Returns:
            The backend multi-series group name. The helper uses this group for
            reconnects without charging usage again.
        """
        callback_to_use = self._resolve_callback(callback)

        self.connect()
        if not self._connected.wait(timeout=timeout):
            raise TimeoutError("Timed out waiting for the SignalR connection to open")

        join_payload = self._normalise_series_requests(series_requests)
        should_parse_datetimes = self.parse_datetimes if parse_datetimes is None else parse_datetimes
        future = self._run_in_loop(
            self._subscribe_join_payload(join_payload, callback_to_use, should_parse_datetimes, timeout=timeout)
        )
        return future.result(timeout=timeout + 5)

    subscribe_to_multi_series = subscribe_to_chart_pushes

    async def async_subscribe_to_chart_pushes(
        self,
        series_requests: list[dict[str, Any]],
        callback: ChartPushCallback | None = None,
        *,
        parse_datetimes: bool | None = None,
        timeout: float = 30,
    ) -> str:
        """Subscribe to chart pushes without blocking the caller's event loop.

        This is the async equivalent of `subscribe_to_chart_pushes()`. It uses
        the same duplicate-subscription cache, so an identical request reuses
        the existing group and updates that subscription's callback.
        """
        callback_to_use = self._resolve_callback(callback)

        await self.async_connect(timeout=timeout)
        connected = await asyncio.to_thread(self._connected.wait, timeout)
        if not connected:
            raise TimeoutError("Timed out waiting for the SignalR connection to open")

        join_payload = self._normalise_series_requests(series_requests)
        should_parse_datetimes = self.parse_datetimes if parse_datetimes is None else parse_datetimes
        return await asyncio.wait_for(
            asyncio.wrap_future(
                self._run_in_loop(
                    self._subscribe_join_payload(join_payload, callback_to_use, should_parse_datetimes, timeout=timeout)
                )
            ),
            timeout + 5,
        )

    def unsubscribe_from_chart_pushes(self, group_name: str, *, timeout: float = 30) -> bool:
        """Stop receiving pushes for one managed multi-series group.

        Args:
            group_name: Group name returned by `subscribe_to_chart_pushes()`.
            timeout: Seconds to wait for the backend `LeaveGroup` call when the
                SignalR connection is currently open.

        Returns:
            `True` when the helper was managing the group and removed it.
            `False` when the group was not known, making repeated or defensive
            calls safe.

        The helper removes its local subscription even if the backend leave
        request cannot be sent because the connection is currently down. That
        prevents future reconnects and lease refreshes for the group.
        """
        self._validate_group_name(group_name)

        if self._loop and self._loop.is_running() and not self._is_running_on_helper_loop():
            future = self._run_in_loop(self._unsubscribe_group(group_name, timeout=timeout))
            return future.result(timeout=timeout + 5)

        return self._remove_subscription(group_name)

    unsubscribe_from_multi_series = unsubscribe_from_chart_pushes
    unsubscribe = unsubscribe_from_chart_pushes

    async def async_unsubscribe_from_chart_pushes(self, group_name: str, *, timeout: float = 30) -> bool:
        """Async version of `unsubscribe_from_chart_pushes()`."""
        self._validate_group_name(group_name)

        if self._loop and self._loop.is_running():
            if self._is_running_on_helper_loop():
                return await self._unsubscribe_group(group_name, timeout=timeout)

            return await asyncio.wait_for(
                asyncio.wrap_future(self._run_in_loop(self._unsubscribe_group(group_name, timeout=timeout))),
                timeout + 5,
            )

        return self._remove_subscription(group_name)

    async_unsubscribe_from_multi_series = async_unsubscribe_from_chart_pushes
    async_unsubscribe = async_unsubscribe_from_chart_pushes

    def close(self, *, wait_for_callbacks: bool = True, timeout: float = 10) -> None:
        """Stop the SignalR connection, lease refresh task and callback workers.

        Known groups are left with `LeaveGroup` first, then the underlying
        websocket is closed before the background task is cancelled so the
        SignalR server can run its disconnect cleanup.

        Args:
            wait_for_callbacks: When true, queued callback work is allowed to
                finish before shutdown. When false, queued work is cancelled.
            timeout: Seconds to wait for shutdown work from the synchronous caller.
        """
        if self._closed:
            return

        self._stop_requested.set()

        if self._loop and self._loop.is_running():
            future = self._run_in_loop(
                self._shutdown_async(
                    wait_for_callbacks=wait_for_callbacks,
                    leave_groups_timeout=max(0.1, min(5.0, timeout)),
                )
            )
            try:
                future.result(timeout=timeout)
            except Exception as exc:
                self.logger.warning("Error during multi-series DPS shutdown: %s", exc)

            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

        if self._callback_executor is not None:
            self._callback_executor.shutdown(wait=wait_for_callbacks, cancel_futures=not wait_for_callbacks)
        self._closed = True

    async def async_close(self, *, wait_for_callbacks: bool = True, timeout: float = 10) -> None:
        """Async version of `close()` for applications already running an event loop.

        Prefer this in async applications so shutdown does not block the
        caller's event loop while the helper drains callback work.
        """
        if self._closed:
            return

        self._stop_requested.set()

        if self._loop and self._loop.is_running():
            await asyncio.wait_for(
                asyncio.wrap_future(
                    self._run_in_loop(
                        self._shutdown_async(
                            wait_for_callbacks=wait_for_callbacks,
                            leave_groups_timeout=max(0.1, min(5.0, timeout)),
                        )
                    )
                ),
                timeout,
            )
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._thread and self._thread.is_alive():
            await asyncio.to_thread(self._thread.join, timeout)

        if self._callback_executor is not None:
            self._callback_executor.shutdown(wait=wait_for_callbacks, cancel_futures=not wait_for_callbacks)
        self._closed = True

    terminate_hub_connection = close

    def _start_loop_thread(self) -> None:
        """Create the private asyncio loop thread used by the synchronous API."""
        self._loop_ready.clear()
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()
        self._loop_ready.wait()

    def _start_loop(self) -> None:
        """Run the private asyncio event loop until shutdown."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    def _is_running_on_helper_loop(self) -> bool:
        """Return true when the caller is already executing on the helper loop."""
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def _resolve_callback(self, callback: ChartPushCallback | None) -> ChartPushCallback:
        """Choose the subscription callback and verify it is callable."""
        callback_to_use = callback if callback is not None else self._default_callback
        if callback_to_use is None:
            raise ValueError("A callback must be provided or registered with on_chart_push")
        if not callable(callback_to_use):
            raise TypeError("callback must be callable")
        return callback_to_use

    def _run_in_loop(self, coro):
        """Submit a coroutine to the helper's private event loop from any thread."""
        if self._loop is None:
            raise RuntimeError("Event loop not ready")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    @staticmethod
    def _install_signalr_missing_method_filter() -> None:
        """Attach the targeted pysignalr missing-method log filter once per process."""
        signalr_logger = logging.getLogger("pysignalr.client")
        with _SIGNALR_LOG_FILTER_LOCK:
            if _SIGNALR_MISSING_METHOD_FILTER not in signalr_logger.filters:
                signalr_logger.addFilter(_SIGNALR_MISSING_METHOD_FILTER)

    async def _ensure_connection_started(self) -> None:
        """Start connection supervision and proactive lease refresh tasks if required."""
        if self._connection_task is None or self._connection_task.done():
            self._connection_task = asyncio.create_task(self._connection_supervisor())
        if (
            self.lease_refresh_interval_days is not None
            and (self._lease_refresh_task is None or self._lease_refresh_task.done())
        ):
            self._lease_refresh_task = asyncio.create_task(self._lease_refresh_loop())

    async def _connection_supervisor(self) -> None:
        """Run the SignalR client and recreate it after unexpected disconnects."""
        delay = self.reconnect_initial_delay_seconds

        while not self._stop_requested.is_set():
            self._build_hub_connection()
            self._client_initialised.set()

            try:
                await self.hub_connection.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.reconnect and not self._stop_requested.is_set():
                    self.logger.warning(
                        "SignalR connection lost; automatically reconnecting in %.1f seconds: %s",
                        delay,
                        exc,
                    )
                else:
                    self.logger.warning("SignalR connection stopped unexpectedly: %s", exc)
            finally:
                self._connected.clear()

            if not self.reconnect or self._stop_requested.is_set():
                break

            if delay:
                await asyncio.sleep(delay)
            delay = min(max(delay * 2, self.reconnect_initial_delay_seconds), self.reconnect_max_delay_seconds)

    def _build_hub_connection(self) -> None:
        """Create a fresh SignalR client wired with auth and lifecycle handlers."""
        api_helper = self._ensure_api_helper()
        access_token_factory, headers = self._build_access_token_factory()

        self.hub_connection = SignalRClient(
            api_helper.endpoints.DPS,
            access_token_factory=access_token_factory,
            headers=headers,
        )
        self._handlers_registered_for_client = set()

        self.hub_connection.on_open(self._on_open)
        self.hub_connection.on_close(self._on_close)
        self.hub_connection.on_error(lambda error: self.logger.warning("SignalR error: %s", error))

    def _ensure_api_helper(self) -> APIHelper:
        """Create the API helper lazily so construction can be deferred in tests/scripts."""
        if self.api_helper is None:
            self.api_helper = APIHelper(self._username, self._public_api_key)
        return self.api_helper

    def _fetch_bearer_token(self) -> str:
        """Refresh and return the bearer token used by SignalR authentication."""
        credentials = self._ensure_api_helper().credentials_holder
        credentials.get_bearer_token()
        return credentials.bearer_token

    def _build_access_token_factory(self) -> tuple[Callable[[], str], dict[str, str]]:
        """Create SignalR auth headers while avoiding a duplicate first token request."""
        first_token = self._fetch_bearer_token()
        first_token_available = True
        token_lock = threading.Lock()

        def access_token_factory() -> str:
            nonlocal first_token_available

            with token_lock:
                if first_token_available:
                    first_token_available = False
                    return first_token

            return self._fetch_bearer_token()

        return access_token_factory, {"Authorization": f"Bearer {first_token}"}

    async def _on_open(self) -> None:
        """Mark the connection open and reconnect existing multi-series groups."""
        self._connected.set()
        subscriptions = list(self._subscriptions.items())
        pre_reconnect_drained = None
        if subscriptions and self._reconnect_callback is not None:
            pre_reconnect_drained = self._pause_push_processing()

        for group_name, _subscription in subscriptions:
            self._register_group_handler(group_name)

        if not subscriptions:
            self._release_push_processing()
            return

        task = asyncio.create_task(self._restore_groups_after_open(subscriptions, pre_reconnect_drained))
        self._reconnect_tasks.add(task)
        task.add_done_callback(self._reconnect_tasks.discard)

    async def _restore_groups_after_open(
        self,
        subscriptions: list[tuple[str, _MultiSeriesSubscription]],
        pre_reconnect_drained: asyncio.Event | None = None,
    ) -> None:
        """Restore groups and run the reconnect callback before pushes resume."""
        release_pushes = False
        try:
            restored = await asyncio.gather(
                *(
                    self._restore_group_until_connected(group_name, subscription)
                    for group_name, subscription in subscriptions
                )
            )
            if not all(restored):
                release_pushes = not self._reconnect_restore_was_interrupted()
                return

            if pre_reconnect_drained is not None:
                await pre_reconnect_drained.wait()
            if self._reconnect_restore_was_interrupted():
                return

            await self._run_reconnect_callback_with_retries()
            if self._reconnect_restore_was_interrupted():
                return

            release_pushes = True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Failed to complete multi-series DPS reconnect restore")
            release_pushes = True
        finally:
            if release_pushes:
                self._release_push_processing()

    def _reconnect_restore_was_interrupted(self) -> bool:
        """Return true when reconnect restore should be retried by the next connection."""
        return self.reconnect and not self._stop_requested.is_set() and not self.is_connected

    def _pause_push_processing(self) -> asyncio.Event | None:
        """Create a closed gate for future pushes and return the previous gate drain signal."""
        gate = self._get_push_processing_gate()
        already_paused = not gate.ready.is_set()
        if already_paused:
            return self._pre_reconnect_drained

        self._push_processing_gate = self._create_push_processing_gate(ready=False)
        self._pre_reconnect_drained = gate.drained
        return self._pre_reconnect_drained

    def _release_push_processing(self) -> None:
        """Allow queued push callbacks to run."""
        self._get_push_processing_gate().ready.set()
        self._pre_reconnect_drained = None

    def _get_push_processing_gate(self) -> _PushProcessingGate:
        """Return the event that gates user push callback execution."""
        if self._push_processing_gate is None:
            self._push_processing_gate = self._create_push_processing_gate(ready=True)
        return self._push_processing_gate

    @staticmethod
    def _create_push_processing_gate(*, ready: bool) -> _PushProcessingGate:
        ready_event = asyncio.Event()
        if ready:
            ready_event.set()

        drained_event = asyncio.Event()
        # A new gate has no queued/running work yet, so it is already drained.
        drained_event.set()
        return _PushProcessingGate(ready=ready_event, drained=drained_event)

    async def _on_close(self) -> None:
        """Mark the connection closed so callers can observe disconnected state."""
        self._connected.clear()
        for task in tuple(self._reconnect_tasks):
            task.cancel()

    async def _join_multi_series(
        self,
        join_payload: list[dict[str, Any]],
        callback: ChartPushCallback,
        parse_datetimes: bool,
        *,
        timeout: float,
        replace_group_name: str | None = None,
        request_key: str | None = None,
    ) -> str:
        """Call `JoinMultiSeries`, store the subscription and register its event handler."""
        response = await self._send_with_response("JoinMultiSeries", [join_payload], timeout=timeout)
        group_name = self._extract_push_name_or_raise(response)
        subscription = _MultiSeriesSubscription(join_payload, callback, parse_datetimes)

        if replace_group_name is not None and replace_group_name != group_name:
            self._subscriptions.pop(replace_group_name, None)
            self._remove_subscription_group_mappings(replace_group_name)

        self._subscriptions[group_name] = subscription
        if request_key is not None:
            self._subscription_group_by_request_key[request_key] = group_name
        self._register_group_handler(group_name)
        return group_name

    async def _subscribe_join_payload(
        self,
        join_payload: list[dict[str, Any]],
        callback: ChartPushCallback,
        parse_datetimes: bool,
        *,
        timeout: float,
    ) -> str:
        """Subscribe to a JoinMultiSeries payload, reusing an existing identical group."""
        request_key = self._get_subscription_request_key(join_payload)
        existing_group = self._subscription_group_by_request_key.get(request_key)
        subscription = _MultiSeriesSubscription(join_payload, callback, parse_datetimes)

        if existing_group in self._subscriptions:
            self._subscriptions[existing_group] = subscription
            self._register_group_handler(existing_group)
            return existing_group

        if existing_group is not None:
            self._subscription_group_by_request_key.pop(request_key, None)

        return await self._join_multi_series(
            join_payload,
            callback,
            parse_datetimes,
            timeout=timeout,
            request_key=request_key,
        )

    async def _unsubscribe_group(self, group_name: str, *, timeout: float) -> bool:
        """Remove local subscription state and leave the SignalR group if connected."""
        if not self._remove_subscription(group_name):
            return False

        await self._leave_signalr_group(group_name, timeout=timeout)
        return True

    async def _reconnect_group(self, group_name: str, subscription: _MultiSeriesSubscription) -> None:
        """Reconnect an existing group without adding usage, rejoining only if needed."""
        try:
            response = await self._send_with_response("ReconnectToPush", [group_name], timeout=30)
            self._raise_reconnect_error_if_present(response)
        except EnactApiError as exc:
            if not self._should_rejoin_after_reconnect_error(exc):
                raise

            self.logger.info("ReconnectToPush cannot restore %s, rejoining: %s", group_name, exc)
            await self._rejoin_group(group_name, subscription)

    async def _restore_group_until_connected(
        self,
        group_name: str,
        subscription: _MultiSeriesSubscription,
    ) -> bool:
        """Keep trying to restore one group while the current connection is open."""
        delay = self.reconnect_initial_delay_seconds

        while not self._stop_requested.is_set() and self.is_connected and group_name in self._subscriptions:
            try:
                current_subscription = self._subscriptions.get(group_name, subscription)
                await self._reconnect_group(group_name, current_subscription)
                return self.is_connected
            except asyncio.CancelledError:
                raise
            except EnactApiError as exc:
                self.logger.warning("Failed to restore multi-series DPS group %s, retrying: %s", group_name, exc)
                if not self.reconnect:
                    return False
            except TimeoutError:
                self.logger.warning("Timed out restoring multi-series DPS group %s, retrying", group_name)
                if not self.reconnect:
                    return False
            except Exception:
                self.logger.exception("Failed to restore multi-series DPS group %s", group_name)
                if not self.reconnect:
                    return False

            await asyncio.sleep(delay if delay > 0 else 0.1)
            delay = min(max(delay * 2, self.reconnect_initial_delay_seconds), self.reconnect_max_delay_seconds)

        return False

    async def _run_reconnect_callback_with_retries(self) -> None:
        """Run the reconnect callback, retrying briefly before queued pushes resume."""
        if self._reconnect_callback is None:
            return

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.reconnect_callback_timeout_seconds
        delay = self.reconnect_initial_delay_seconds

        while not self._stop_requested.is_set() and self.is_connected:
            remaining = deadline - loop.time()
            if remaining <= 0:
                self.logger.warning(
                    "Reconnect callback did not complete within %.1f seconds; releasing queued multi-series pushes",
                    self.reconnect_callback_timeout_seconds,
                )
                return

            try:
                await asyncio.wait_for(self._run_reconnect_callback(), timeout=remaining)
                return
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                self.logger.warning(
                    "Reconnect callback did not complete within %.1f seconds; releasing queued multi-series pushes",
                    self.reconnect_callback_timeout_seconds,
                )
                return
            except Exception as exc:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    self.logger.exception(
                        "Reconnect callback failed for %.1f seconds; releasing queued multi-series pushes",
                        self.reconnect_callback_timeout_seconds,
                    )
                    return

                retry_delay = delay if delay > 0 else min(0.1, remaining)
                self.logger.warning("Reconnect callback failed; retrying before queued pushes resume: %s", exc)
                await asyncio.sleep(min(retry_delay, remaining))
                delay = min(max(delay * 2, self.reconnect_initial_delay_seconds), self.reconnect_max_delay_seconds)

    async def _run_reconnect_callback(self) -> None:
        """Invoke a sync or async reconnect callback without blocking the event loop."""
        callback = self._reconnect_callback
        if callback is None:
            return

        if inspect.iscoroutinefunction(callback):
            result = self._invoke_reconnect_callback(callback)
        else:
            result = await asyncio.to_thread(self._invoke_reconnect_callback, callback)

        if inspect.isawaitable(result):
            await result

    def _invoke_reconnect_callback(self, callback: ReconnectCallback) -> Any:
        """Call the reconnect callback with this helper when its signature accepts it."""
        if self._callback_accepts_one_argument(callback):
            return callback(self)
        return callback()

    @staticmethod
    def _should_rejoin_after_reconnect_error(exc: EnactApiError) -> bool:
        """Return true only for errors that mean the previous push can no longer be restored."""
        error_code = str(exc.error_code or "").lower()
        if error_code == "reconnectunavailable":
            return True

        message = str(exc.message or "").lower()
        error_text = f"{error_code} {message}"
        non_reconnectable_terms = (
            "cannot reconnect",
            "does not exist",
            "expired",
            "invalid group",
            "invalid push",
            "no longer",
            "not found",
            "unknown group",
            "unknown push",
        )
        return any(term in error_text for term in non_reconnectable_terms)

    @staticmethod
    def _raise_reconnect_error_if_present(response: Any) -> None:
        """Raise only for explicit reconnect errors; no reconnect payload still means success."""
        messages = MultiSeriesDPSHelper._get_case_insensitive(response, "messages") or []
        for message in messages:
            error_code = MultiSeriesDPSHelper._get_case_insensitive(message, "errorCode")
            error_message = MultiSeriesDPSHelper._get_case_insensitive(message, "message")
            if error_code or error_message:
                raise EnactApiError(error_code or "SignalRError", error_message or "SignalR request failed", response)

    async def _rejoin_group(self, group_name: str, subscription: _MultiSeriesSubscription) -> str:
        """Recreate a multi-series group after reconnect fails or the lease expires."""
        return await self._join_multi_series(
            subscription.join_payload,
            subscription.callback,
            subscription.parse_datetimes,
            timeout=30,
            replace_group_name=group_name,
            request_key=self._get_subscription_request_key(subscription.join_payload),
        )

    async def _lease_refresh_loop(self) -> None:
        """Renew multi-series group leases before the backend's 14-day expiry."""
        refresh_interval_seconds = self.lease_refresh_interval_days * 24 * 60 * 60

        while not self._stop_requested.is_set():
            await asyncio.sleep(refresh_interval_seconds)
            if self._stop_requested.is_set():
                return
            if not self.is_connected:
                continue

            await self._refresh_all_subscription_leases()

    async def _refresh_all_subscription_leases(self) -> None:
        """Refresh every known subscription lease by rejoining its original request."""
        for group_name, subscription in list(self._subscriptions.items()):
            try:
                await self._join_multi_series(
                    subscription.join_payload,
                    subscription.callback,
                    subscription.parse_datetimes,
                    timeout=30,
                    replace_group_name=group_name,
                    request_key=self._get_subscription_request_key(subscription.join_payload),
                )
            except Exception:
                self.logger.exception("Failed to refresh multi-series DPS lease for %s", group_name)

    async def _send_with_response(self, method: str, arguments: list[Any], *, timeout: float) -> Any:
        """Send a SignalR hub method invocation and wait for its callback response."""
        if self.hub_connection is None:
            raise RuntimeError("SignalR client is not initialised")

        response_future = asyncio.get_running_loop().create_future()

        async def on_response(message) -> None:
            error = getattr(message, "error", None)
            if error:
                if not response_future.done():
                    response_future.set_exception(EnactApiError("SignalRError", error, message))
                return

            result = getattr(message, "result", message)
            if not response_future.done():
                response_future.set_result(result)

        try:
            await self.hub_connection.send(method, arguments, on_response)
            return await asyncio.wait_for(response_future, timeout=timeout)
        except Exception:
            self._remove_pending_invocation_handler(on_response)
            raise

    def _remove_pending_invocation_handler(self, callback: Callable[[Any], Any]) -> None:
        """Best-effort cleanup for a SignalR invocation callback after a failed wait."""
        invocation_handlers = getattr(self.hub_connection, "_invocation_handlers", None)
        if not isinstance(invocation_handlers, dict):
            return

        for invocation_id, handler in list(invocation_handlers.items()):
            if handler is callback:
                invocation_handlers.pop(invocation_id, None)

    def _register_group_handler(self, group_name: str) -> None:
        """Register the dynamic SignalR event that receives pushes for a group."""
        if self.hub_connection is None or group_name in self._handlers_registered_for_client:
            return

        async def push_handler(*args) -> None:
            payload = args[0] if len(args) == 1 else list(args)
            await self._handle_chart_push(group_name, payload)

        self.hub_connection.on(group_name, push_handler)
        self._handlers_registered_for_client.add(group_name)

    async def _handle_chart_push(self, group_name: str, payload: Any) -> None:
        """Convert a raw SignalR push into callback arguments and enqueue it."""
        subscription = self._subscriptions.get(group_name)
        if subscription is None:
            return

        self._sequence += 1
        frame, metadata = self._series_ping_to_frame_and_metadata(
            payload,
            sequence=self._sequence,
            group_name=group_name,
            parse_datetimes=subscription.parse_datetimes,
        )
        await self._enqueue_callback(subscription.callback, frame, metadata)

    async def _enqueue_callback(
        self,
        callback: ChartPushCallback,
        frame: pd.DataFrame,
        metadata: MultiSeriesPushMetadata,
    ) -> None:
        """Queue callback work on the partition for its push stream."""
        self._ensure_callback_processing()
        if self._callback_slots is not None:
            await self._callback_slots.acquire()

        partition_key = self._get_callback_partition_key(metadata)
        shard_index = self._get_callback_shard_index(partition_key, len(self._callback_queues))
        push_processing_gate = self._get_push_processing_gate()
        self._mark_push_processing_started(push_processing_gate)
        try:
            await self._callback_queues[shard_index].put((callback, frame, metadata, push_processing_gate))
        except Exception:
            self._mark_push_processing_finished(push_processing_gate)
            if self._callback_slots is not None:
                self._callback_slots.release()
            raise

    def _ensure_callback_processing(self) -> None:
        """Initialise callback concurrency controls on the helper's event loop."""
        if not self._callback_queues:
            self._callback_queues = [asyncio.Queue() for _ in range(self._callback_worker_count)]
            self._callback_tasks = [
                asyncio.create_task(self._drain_callback_shard(queue))
                for queue in self._callback_queues
            ]
        if self.callback_queue_maxsize and self._callback_slots is None:
            self._callback_slots = asyncio.Semaphore(self.callback_queue_maxsize)

    @staticmethod
    def _get_callback_partition_key(metadata: MultiSeriesPushMetadata) -> str:
        """Return the key that keeps one push stream ordered on a single worker."""
        return metadata.push_id or metadata.series_id or metadata.group_name

    @staticmethod
    def _get_callback_shard_index(partition_key: str, shard_count: int) -> int:
        """Map a partition key to a stable callback worker shard."""
        if shard_count < 1:
            raise ValueError("shard_count must be at least 1")
        return zlib.crc32(partition_key.encode("utf-8")) % shard_count

    async def _drain_callback_shard(self, queue: asyncio.Queue) -> None:
        """Process one fixed callback shard sequentially."""
        while True:
            callback, frame, metadata, push_processing_gate = await queue.get()
            try:
                await push_processing_gate.ready.wait()
                await self._run_user_callback(callback, frame, metadata)
            except Exception:
                self.logger.exception("Error in multi-series DPS callback for sequence %s", metadata.sequence)
            finally:
                self._mark_push_processing_finished(push_processing_gate)
                queue.task_done()
                if self._callback_slots is not None:
                    self._callback_slots.release()

    def _mark_push_processing_started(self, gate: _PushProcessingGate) -> None:
        gate.pending += 1
        gate.drained.clear()

    def _mark_push_processing_finished(self, gate: _PushProcessingGate) -> None:
        gate.pending -= 1
        if gate.pending <= 0:
            gate.pending = 0
            gate.drained.set()

    async def _run_user_callback(
        self,
        callback: ChartPushCallback,
        frame: pd.DataFrame,
        metadata: MultiSeriesPushMetadata,
    ) -> None:
        """Invoke async callbacks directly and sync callbacks in the bounded thread pool."""
        if inspect.iscoroutinefunction(callback):
            await self._invoke_callback(callback, frame, metadata)
            return

        loop = asyncio.get_running_loop()
        executor = self._get_callback_executor()
        result = await loop.run_in_executor(executor, self._invoke_callback, callback, frame, metadata)
        if inspect.isawaitable(result):
            await result

    def _get_callback_executor(self) -> ThreadPoolExecutor:
        """Create the bounded thread pool used only for synchronous callbacks."""
        if self._callback_executor is None:
            self._callback_executor = ThreadPoolExecutor(max_workers=self._callback_worker_count)
        return self._callback_executor

    @staticmethod
    def _invoke_callback(callback: ChartPushCallback, frame: pd.DataFrame, metadata: MultiSeriesPushMetadata) -> Any:
        """Call the user callback with metadata when its signature accepts it."""
        if MultiSeriesDPSHelper._callback_accepts_metadata(callback):
            return callback(frame, metadata)
        return callback(frame)

    @staticmethod
    def _callback_accepts_metadata(callback: ChartPushCallback) -> bool:
        """Return true when a callback can accept `(dataframe, metadata)`."""
        return MultiSeriesDPSHelper._callback_accepts_positional_count(callback, 2)

    @staticmethod
    def _callback_accepts_one_argument(callback: Callable[..., Any]) -> bool:
        """Return true when a callback can accept one positional argument."""
        return MultiSeriesDPSHelper._callback_accepts_positional_count(callback, 1)

    @staticmethod
    def _callback_accepts_positional_count(callback: Callable[..., Any], count: int) -> bool:
        """Return true when a callback can accept at least `count` positional arguments."""
        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            return True

        positional = [
            parameter
            for parameter in signature.parameters.values()
            if parameter.kind
            in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(
            parameter.kind == inspect.Parameter.VAR_POSITIONAL
            for parameter in signature.parameters.values()
        )
        return has_varargs or len(positional) >= count

    async def _shutdown_async(self, *, wait_for_callbacks: bool, leave_groups_timeout: float = 5.0) -> None:
        """Close SignalR, cancel background tasks and optionally drain callbacks."""
        await self._leave_all_subscribed_groups(timeout=leave_groups_timeout)
        await self._close_hub_connection()

        if self._lease_refresh_task is not None and not self._lease_refresh_task.done():
            self._lease_refresh_task.cancel()
            await asyncio.gather(self._lease_refresh_task, return_exceptions=True)

        for task in tuple(self._reconnect_tasks):
            task.cancel()
        if self._reconnect_tasks:
            await asyncio.gather(*self._reconnect_tasks, return_exceptions=True)
            self._reconnect_tasks.clear()

        if self._connection_task is not None and not self._connection_task.done():
            self._connection_task.cancel()
            await asyncio.gather(self._connection_task, return_exceptions=True)

        self._release_push_processing()

        if wait_for_callbacks:
            for queue in self._callback_queues:
                await queue.join()

        for task in self._callback_tasks:
            task.cancel()
        await asyncio.gather(*self._callback_tasks, return_exceptions=True)

        self._callback_tasks.clear()
        self._callback_queues.clear()
        self._connected.clear()

    async def _leave_all_subscribed_groups(self, *, timeout: float) -> None:
        """Best-effort leave of all known backend groups before disconnecting."""
        group_names = tuple(self._subscriptions.keys())
        if not group_names:
            return

        self._subscriptions.clear()
        self._subscription_group_by_request_key.clear()

        await asyncio.gather(
            *(self._leave_signalr_group(group_name, timeout=timeout) for group_name in group_names),
            return_exceptions=True,
        )

    async def _leave_signalr_group(self, group_name: str, *, timeout: float) -> None:
        """Best-effort backend `LeaveGroup` call for one SignalR group."""
        if self.hub_connection is None or not self.is_connected:
            return

        try:
            await self._send_with_response("LeaveGroup", [group_name], timeout=timeout)
        except Exception:
            self.logger.warning("Failed to leave multi-series DPS group %s", group_name, exc_info=True)

    async def _close_hub_connection(self) -> None:
        """Best-effort close of the underlying SignalR websocket before task cancellation."""
        connection = self.hub_connection
        if connection is None:
            return

        for method_name in ("close", "stop", "disconnect"):
            method = getattr(connection, method_name, None)
            if method is None:
                continue
            result = method()
            if inspect.isawaitable(result):
                await result
            return

        transport = getattr(connection, "_transport", None)
        websocket = getattr(transport, "_ws", None)
        close = getattr(websocket, "close", None)
        if close is None:
            return

        result = close()
        if inspect.isawaitable(result):
            await result

    def _normalise_series_requests(self, series_requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate and normalise caller input to the backend JoinMultiSeries shape."""
        if not isinstance(series_requests, list) or not series_requests:
            raise ValueError("series_requests must be a non-empty list of dictionaries")

        join_payload: list[dict[str, Any]] = []
        for series_entry in series_requests:
            if not isinstance(series_entry, dict):
                raise ValueError("Each series request must be a dictionary")

            series_id = series_entry.get("seriesId") or series_entry.get("SeriesId")
            if not isinstance(series_id, str):
                raise ValueError("Each series request must include a string seriesId")

            country_id = series_entry.get("countryId", series_entry.get("CountryId", "Gb"))
            if not isinstance(country_id, str):
                raise ValueError(f"countryId for series {series_id} must be a string")

            series_payload: dict[str, Any] = {"seriesId": series_id, "countryId": country_id}

            option_ids = series_entry.get("optionIds", series_entry.get("OptionIds"))
            if option_ids is not None:
                if not is_2d_list_of_strings(option_ids):
                    raise ValueError(
                        f"optionIds for series {series_id} must be a 2-dimensional list of strings, "
                        "for example [['Coal'], ['Wind']]"
                    )
                series_payload["optionIds"] = [list(option_group) for option_group in option_ids]

            join_payload.append(series_payload)

        return join_payload

    @staticmethod
    def _get_subscription_request_key(join_payload: list[dict[str, Any]]) -> str:
        """Return a stable key used to avoid duplicate JoinMultiSeries calls."""
        canonical_payload = []
        for series_entry in join_payload:
            canonical_entry: dict[str, Any] = {
                "countryId": series_entry["countryId"],
                "seriesId": series_entry["seriesId"],
            }

            option_ids = series_entry.get("optionIds")
            if option_ids is not None:
                canonical_entry["optionIds"] = sorted(list(option_group) for option_group in option_ids)

            canonical_payload.append(canonical_entry)

        canonical_payload.sort(key=lambda entry: json.dumps(entry, sort_keys=True, separators=(",", ":")))
        return json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))

    def _remove_subscription_group_mappings(self, group_name: str) -> None:
        """Remove duplicate-subscription cache entries pointing at a replaced group."""
        for request_key, mapped_group_name in list(self._subscription_group_by_request_key.items()):
            if mapped_group_name == group_name:
                self._subscription_group_by_request_key.pop(request_key, None)

    def _remove_subscription(self, group_name: str) -> bool:
        """Remove a managed group locally without making a SignalR call."""
        if self._subscriptions.pop(group_name, None) is None:
            return False

        self._remove_subscription_group_mappings(group_name)
        return True

    @staticmethod
    def _validate_group_name(group_name: str) -> None:
        """Validate a user-supplied SignalR group name."""
        if not isinstance(group_name, str) or not group_name:
            raise ValueError("group_name must be a non-empty string")

    @staticmethod
    def _extract_push_name_or_raise(response: dict[str, Any]) -> str:
        """Read `data.pushName` from a SignalR response or raise an EnactApiError."""
        messages = MultiSeriesDPSHelper._get_case_insensitive(response, "messages") or []
        for message in messages:
            error_code = MultiSeriesDPSHelper._get_case_insensitive(message, "errorCode")
            error_message = MultiSeriesDPSHelper._get_case_insensitive(message, "message")
            if error_code or error_message:
                raise EnactApiError(error_code or "SignalRError", error_message or "SignalR request failed", response)

        data = MultiSeriesDPSHelper._get_case_insensitive(response, "data") or {}
        push_name = MultiSeriesDPSHelper._get_case_insensitive(data, "pushName")
        if not push_name:
            raise EnactApiError("DataFormatError", "SignalR response did not include a pushName", response)
        return push_name

    @staticmethod
    def _series_ping_to_frame_and_metadata(
        payload: Any,
        *,
        sequence: int,
        group_name: str,
        parse_datetimes: bool,
    ) -> tuple[pd.DataFrame, MultiSeriesPushMetadata]:
        """Convert a backend SeriesPing payload into a dataframe and metadata object."""
        series_ping = MultiSeriesDPSHelper._coerce_signalr_payload(payload)
        ping_data = MultiSeriesDPSHelper._get_case_insensitive(series_ping, "data") or {}
        push_id = MultiSeriesDPSHelper._get_case_insensitive(ping_data, "id")
        clean_push_id = MultiSeriesDPSHelper._strip_chart_update_prefix(push_id)
        country_id, series_id, option_ids = MultiSeriesDPSHelper._parse_push_id(clean_push_id)
        plant = MultiSeriesDPSHelper._extract_plant_metadata(series_ping)

        metadata = MultiSeriesPushMetadata(
            sequence=sequence,
            received_at_utc=datetime.now(timezone.utc),
            group_name=group_name,
            push_id=clean_push_id,
            series_id=series_id,
            country_id=country_id,
            option_ids=option_ids,
            day=MultiSeriesDPSHelper._get_case_insensitive(ping_data, "day"),
            replace_series=bool(MultiSeriesDPSHelper._get_case_insensitive(ping_data, "replaceSeries")),
            refresh=bool(MultiSeriesDPSHelper._get_case_insensitive(series_ping, "refresh")),
            plant=plant,
            raw=series_ping if isinstance(series_ping, dict) else {},
        )

        changes = MultiSeriesDPSHelper._get_case_insensitive(ping_data, "data") or []
        frame = MultiSeriesDPSHelper._changes_to_dataframe(changes, metadata, parse_datetimes)
        frame.attrs["enact_push_metadata"] = metadata
        return frame, metadata

    @staticmethod
    def _changes_to_dataframe(
        changes: list[dict[str, Any]],
        metadata: MultiSeriesPushMetadata,
        parse_datetimes: bool,
    ) -> pd.DataFrame:
        """Build a time-indexed dataframe from SeriesPing point-change objects."""
        frame = pd.DataFrame()
        frame.index.name = "DateTime"

        for change in changes:
            current = MultiSeriesDPSHelper._get_case_insensitive(change, "current") or {}
            timestamp = MultiSeriesDPSHelper._get_timestamp_from_change(current, parse_datetimes)
            if timestamp is None:
                continue

            values = MultiSeriesDPSHelper._get_values_from_change(change, current)
            if MultiSeriesDPSHelper._get_case_insensitive(change, "deletePoint"):
                values = [pd.NA for _ in values] or [pd.NA]

            column_names = MultiSeriesDPSHelper._get_value_column_names(metadata, len(values))
            for column_name, value in zip(column_names, values):
                frame.loc[timestamp, column_name] = value

        return frame

    @staticmethod
    def _get_timestamp_from_change(current: dict[str, Any], parse_datetimes: bool) -> Any:
        """Extract a UTC timestamp from a point change in pandas or ISO-string form."""
        date_period = MultiSeriesDPSHelper._get_case_insensitive(current, "datePeriod") or {}
        timestamp = MultiSeriesDPSHelper._get_case_insensitive(date_period, "datePeriodCombinedGmt")

        if timestamp is None:
            array_point = MultiSeriesDPSHelper._get_case_insensitive(current, "arrayPoint") or []
            if array_point:
                timestamp = array_point[0]

        if timestamp is None:
            object_point = MultiSeriesDPSHelper._get_case_insensitive(current, "objectPoint") or {}
            timestamp = MultiSeriesDPSHelper._get_case_insensitive(object_point, "x")

        if timestamp is None:
            return None

        if isinstance(timestamp, (int, float)):
            parsed = pd.to_datetime(timestamp, unit="ms", utc=True)
            return parsed if parse_datetimes else parsed.isoformat().replace("+00:00", "Z")

        timestamp_string = str(timestamp)
        if not timestamp_string.endswith("Z") and "+" not in timestamp_string[-6:]:
            timestamp_string += "Z"

        if parse_datetimes:
            return pd.to_datetime(timestamp_string, utc=True)
        return timestamp_string

    @staticmethod
    def _get_values_from_change(change: dict[str, Any], current: dict[str, Any]) -> list[Any]:
        """Extract one or more y-values from array or object point formats."""
        array_point = MultiSeriesDPSHelper._get_case_insensitive(current, "arrayPoint")
        if isinstance(array_point, list) and len(array_point) > 1:
            return array_point[1:]

        object_point = MultiSeriesDPSHelper._get_case_insensitive(current, "objectPoint") or {}
        if isinstance(object_point, dict):
            for key in ("y", "value"):
                value = MultiSeriesDPSHelper._get_case_insensitive(object_point, key)
                if value is not None:
                    return [value]
            return [
                value
                for key, value in object_point.items()
                if key.lower() not in {"x", "name", "dateperiod"}
            ]

        current_value = MultiSeriesDPSHelper._get_case_insensitive(change, "value")
        return [current_value] if current_value is not None else [pd.NA]

    @staticmethod
    def _get_value_column_names(metadata: MultiSeriesPushMetadata, value_count: int) -> list[str]:
        """Create dataframe column names for single-value and multi-value points."""
        base_column = metadata.push_id or metadata.series_id or "value"
        if value_count <= 1:
            return [base_column]
        return [f"{base_column}_{index}" for index in range(value_count)]

    @staticmethod
    def _coerce_signalr_payload(payload: Any) -> dict[str, Any]:
        """Normalise SignalR callback arguments to the SeriesPing dictionary shape."""
        if isinstance(payload, tuple):
            payload = list(payload)
        if isinstance(payload, list):
            if len(payload) == 1:
                return MultiSeriesDPSHelper._coerce_signalr_payload(payload[0])
            return {"data": payload}
        if isinstance(payload, dict):
            return payload
        return {}

    @staticmethod
    def _extract_plant_metadata(series_ping: dict[str, Any]) -> PlantPushMetadata | None:
        """Extract optional plant metadata attached to plant series pushes."""
        plant = MultiSeriesDPSHelper._get_case_insensitive(series_ping, "plantMeta")
        if not isinstance(plant, dict):
            return None

        return PlantPushMetadata(
            plant_id=MultiSeriesDPSHelper._get_case_insensitive(plant, "plantId"),
            owner=MultiSeriesDPSHelper._get_case_insensitive(plant, "owner"),
            fuel=MultiSeriesDPSHelper._get_case_insensitive(plant, "fuel"),
            zone=MultiSeriesDPSHelper._get_case_insensitive(plant, "zone"),
            raw=plant,
        )

    @staticmethod
    def _strip_chart_update_prefix(push_id: str | None) -> str | None:
        """Remove the SignalR chart-update prefix from a push id when present."""
        if push_id is None:
            return None
        prefix = "chartUpdate&"
        return push_id[len(prefix):] if push_id.startswith(prefix) else push_id

    @staticmethod
    def _parse_push_id(push_id: str | None) -> tuple[str | None, str | None, tuple[str, ...]]:
        """Split a backend push id into country, series and option ids."""
        if not push_id:
            return None, None, ()

        parts = push_id.split("&")
        if len(parts) == 1:
            return None, parts[0], ()

        country_id = parts[0]
        series_id = parts[1]
        option_ids: tuple[str, ...] = ()
        if len(parts) > 2 and parts[2] and parts[2] != "none":
            option_ids = tuple(option for option in parts[2].split("*") if option and option != "none")

        return country_id, series_id, option_ids

    @staticmethod
    def _get_case_insensitive(source: Any, key: str) -> Any:
        """Read a dictionary value without depending on JSON property casing."""
        if not isinstance(source, dict):
            return None
        if key in source:
            return source[key]

        lower_key = key.lower()
        for candidate_key, value in source.items():
            if isinstance(candidate_key, str) and candidate_key.lower() == lower_key:
                return value
        return None
