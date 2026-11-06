from __future__ import annotations

import asyncio
import copy
import inspect
import logging
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from time import monotonic
from typing import Any, Callable

from lcp_delta.common.http.exceptions import EnactApiError
from lcp_delta.enact._signalr_connection import _SignalRConnection


ACCEPTED_BOA_PUSH_NAME = "acceptedBoaUpdated"


@dataclass(frozen=True)
class AcceptedBoaPush:
    """One accepted-row batch. Revision is source metadata, not a delivery sequence.

    Changed collections contain complete rows to insert/replace; removed
    collections contain previous rows to remove. Row identity is scoped to
    settlement date, period, bid/offer side, and the row's ``deltaId``.
    """

    date: date
    period: int
    revision: int
    accepted_bids: tuple[dict[str, Any], ...]
    accepted_offers: tuple[dict[str, Any], ...]
    removed_accepted_bids: tuple[dict[str, Any], ...]
    removed_accepted_offers: tuple[dict[str, Any], ...]

    @classmethod
    def from_payload(cls, payload: Any) -> AcceptedBoaPush:
        """Decode a public SignalR batch without fetching or accumulating data."""
        while isinstance(payload, (list, tuple)) and len(payload) == 1:
            payload = payload[0]
        if not isinstance(payload, dict):
            raise ValueError("Expected an accepted BOA object")
        settlement_date = date.fromisoformat(payload["date"])
        period, revision = payload["period"], payload["revision"]
        if type(period) is not int or not 1 <= period <= 50:
            raise ValueError("Invalid settlement period")
        if type(revision) is not int or revision < 0:
            raise ValueError("Invalid source revision")

        def rows(key):
            values = payload[key]
            if not isinstance(values, list) or any(
                not isinstance(row, dict) or not isinstance(row.get("deltaId"), str) or not row["deltaId"]
                for row in values
            ):
                raise ValueError("Invalid accepted BOA row collection")
            return tuple(copy.deepcopy(values))

        return cls(
            settlement_date,
            period,
            revision,
            rows("acceptedBids"),
            rows("acceptedOffers"),
            rows("removedAcceptedBids"),
            rows("removedAcceptedOffers"),
        )


class AcceptedBoaDPSHelper(_SignalRConnection):
    """Receive batches of live GB accepted BOA changes on one SignalR connection.

    Requires the same live GB entitlement as the public BOA API. A callback
    accepts one ``AcceptedBoaPush`` and may be synchronous or asynchronous.
    Callbacks run sequentially. A finite ``callback_queue_maxsize`` bounds
    queued plus running callbacks and applies backpressure (0 is unbounded).

    The engine broadcasts a heartbeat every five seconds. A missing heartbeat
    warns once after ``heartbeat_timeout_seconds`` (default 30); receipt after
    a warning logs recovery once. This monitors engine-to-client delivery,
    independently of the SignalR connection. Set the timeout to None to disable.

    Reconnect restores future updates only: there is no initial snapshot,
    replay, revision deduplication, automatic HTTP fetch, or reload handling.
    Close the helper or use its synchronous context manager on shutdown.
    """

    def __init__(
        self,
        username: str,
        public_api_key: str,
        callback: Callable[[AcceptedBoaPush], Any] | None = None,
        *,
        auto_connect: bool = True,
        reconnect: bool = True,
        reconnect_initial_delay_seconds: float = 2.0,
        reconnect_max_delay_seconds: float = 30.0,
        callback_queue_maxsize: int = 0,
        heartbeat_timeout_seconds: float | None = 30.0,
        logger: logging.Logger | None = None,
    ):
        if callback is not None and not callable(callback):
            raise TypeError("callback must be callable")
        if callback_queue_maxsize < 0:
            raise ValueError("callback_queue_maxsize cannot be negative")
        if heartbeat_timeout_seconds is not None and (
            not math.isfinite(heartbeat_timeout_seconds) or heartbeat_timeout_seconds <= 0
        ):
            raise ValueError("heartbeat_timeout_seconds must be a positive finite number or None")
        if reconnect_initial_delay_seconds < 0 or reconnect_max_delay_seconds < reconnect_initial_delay_seconds:
            raise ValueError("Invalid reconnect delays")
        self._username, self._public_api_key = username, public_api_key
        self.api_helper = None
        self.logger = logger or logging.getLogger(__name__)
        self.reconnect = reconnect
        self.reconnect_initial_delay_seconds = reconnect_initial_delay_seconds
        self.reconnect_max_delay_seconds = reconnect_max_delay_seconds
        self.callback_queue_maxsize = callback_queue_maxsize
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._default_callback = callback
        self._callback = callback
        self._subscribed = False
        self._closed = False
        self._loop = None
        self._thread = None
        self._loop_ready = threading.Event()
        self._client_initialised = threading.Event()
        self._connected = threading.Event()
        self._stop_requested = threading.Event()
        self._connection_task = None
        self._restore_task = None
        self._heartbeat_task = None
        self._heartbeat_monitor_started_at = None
        self._last_heartbeat_monotonic = None
        self._heartbeat_overdue = False
        self.hub_connection = None
        self._handlers_registered_for_client = set()
        self._subscription_lock = None
        self._queue = None
        self._callback_slots = None
        self._callback_task = None
        self._callback_executor = None
        if auto_connect:
            self.connect()

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def group_names(self) -> tuple[str, ...]:
        return (ACCEPTED_BOA_PUSH_NAME,) if self._subscribed else ()

    def __enter__(self) -> AcceptedBoaDPSHelper:
        self.connect()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def subscribe_to_accepted_boa_updates(self, callback=None, *, timeout: float = 30) -> str:
        """Subscribe to future GB accepted-row batches, reusing an existing subscription."""
        if self._is_running_on_helper_loop():
            raise RuntimeError("Use async_subscribe_to_accepted_boa_updates on the helper's event loop")
        callback = self._callback if callback is None else callback
        if callback is None:
            callback = self._default_callback
        if not callable(callback):
            raise ValueError("A callable callback is required")
        self.connect(timeout=timeout)
        if not self._connected.wait(timeout):
            raise TimeoutError("Timed out waiting for the SignalR connection to open")
        return self._run_in_loop(self._subscribe(callback, timeout)).result(timeout=timeout + 5)

    async def async_subscribe_to_accepted_boa_updates(self, callback=None, *, timeout: float = 30) -> str:
        """Subscribe without blocking the caller's event loop."""
        return await asyncio.to_thread(self.subscribe_to_accepted_boa_updates, callback, timeout=timeout)

    async def _ensure_connection_started(self) -> None:
        if self._connection_task is None or self._connection_task.done():
            self._connection_task = asyncio.create_task(self._connection_supervisor())

    async def _on_open(self) -> None:
        if self._restore_task is not None:
            self._restore_task.cancel()
        await self._stop_heartbeat_monitor()
        self._connected.set()
        self._register_handler()
        self.logger.info("SignalR connection opened.")
        if self._subscribed:
            self.logger.info("Restoring accepted BOA subscription...")
            self._start_heartbeat_monitor()
            self._restore_task = asyncio.create_task(self._restore_subscription())

    async def _on_close(self) -> None:
        was_connected = self.is_connected
        self._connected.clear()
        await self._stop_heartbeat_monitor()
        if self._restore_task is not None:
            self._restore_task.cancel()
        if was_connected and not self._stop_requested.is_set():
            self.logger.warning("SignalR connection closed; accepted BOA updates may be missed while disconnected.")

    def _register_handler(self) -> None:
        handlers = {
            ACCEPTED_BOA_PUSH_NAME: self._handle_batch,
            "heartbeat": self._handle_heartbeat,
            "zeroPnUpdate": self._ignore_broadcast,
            "earliestDateInBuffer": self._ignore_broadcast,
        }
        for name, handler in handlers.items():
            if name not in self._handlers_registered_for_client:
                self.hub_connection.on(name, handler)
                self._handlers_registered_for_client.add(name)

    @staticmethod
    async def _ignore_broadcast(*_args) -> None:
        """Ignore known server-wide broadcasts unrelated to this feed."""
        pass

    async def _handle_heartbeat(self, *_args) -> None:
        """Measure local receipt time; server timestamp formats and clock skew are irrelevant."""
        if not self.is_connected or self._stop_requested.is_set():
            return
        self._last_heartbeat_monotonic = monotonic()
        if self._heartbeat_overdue:
            self._heartbeat_overdue = False
            self.logger.info("Enact heartbeat resumed; live heartbeat delivery has recovered.")

    def _start_heartbeat_monitor(self) -> None:
        if self.heartbeat_timeout_seconds is None:
            return
        self._heartbeat_monitor_started_at = monotonic()
        self._last_heartbeat_monotonic = None
        self._heartbeat_overdue = False
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._monitor_heartbeat())

    async def _stop_heartbeat_monitor(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None
        self._heartbeat_monitor_started_at = None
        self._last_heartbeat_monotonic = None
        self._heartbeat_overdue = False

    async def _monitor_heartbeat(self) -> None:
        while True:
            await asyncio.sleep(min(5.0, self.heartbeat_timeout_seconds))
            self._check_heartbeat()

    def _check_heartbeat(self) -> None:
        if (
            self.heartbeat_timeout_seconds is None
            or self._heartbeat_monitor_started_at is None
            or not self._subscribed
            or not self.is_connected
            or self._stop_requested.is_set()
        ):
            return
        last_received = self._last_heartbeat_monotonic
        if last_received is None:
            last_received = self._heartbeat_monitor_started_at
        age = monotonic() - last_received
        if age >= self.heartbeat_timeout_seconds and not self._heartbeat_overdue:
            self._heartbeat_overdue = True
            self.logger.warning(
                "Enact heartbeat overdue: no heartbeat received for %.1f seconds; live updates may be stale.", age
            )

    def _get_subscription_lock(self):
        if self._subscription_lock is None:
            self._subscription_lock = asyncio.Lock()
        return self._subscription_lock

    async def _subscribe(self, callback, timeout) -> str:
        async with self._get_subscription_lock():
            self._callback = callback
            if not self._subscribed:
                self._register_handler()
                await self._join(timeout)
                self._subscribed = True
                self._start_heartbeat_monitor()
                self.logger.info("Subscribed to accepted BOA updates.")
            return ACCEPTED_BOA_PUSH_NAME

    async def _join(self, timeout) -> None:
        response = await self._send_with_response("JoinAcceptedBoaPush", [], timeout=timeout)
        self._check_response(response)
        if response.get("data", {}).get("pushName") != ACCEPTED_BOA_PUSH_NAME:
            raise EnactApiError("DataFormatError", "Unexpected accepted BOA subscription response", response)

    @staticmethod
    def _check_response(response) -> None:
        if not isinstance(response, dict):
            raise EnactApiError("DataFormatError", "Invalid accepted BOA subscription response", response)
        for message in response.get("messages", []):
            if message.get("errorCode"):
                raise EnactApiError(message["errorCode"], message.get("message", "Subscription failed"), response)

    async def _restore_subscription(self) -> None:
        delay = self.reconnect_initial_delay_seconds
        while self._subscribed and self.is_connected and not self._stop_requested.is_set():
            try:
                async with self._get_subscription_lock():
                    if not self._subscribed:
                        return
                    try:
                        response = await self._send_with_response(
                            "ReconnectToPush", [ACCEPTED_BOA_PUSH_NAME], timeout=30
                        )
                        self._check_response(response)
                    except EnactApiError as exc:
                        if exc.error_code not in {"DataFormatError", "ReconnectUnavailable"}:
                            raise
                        await self._join(30)
                    if not self.is_connected or self._stop_requested.is_set():
                        return
                    self.logger.info(
                        "Accepted BOA subscription restored; listening for future updates. "
                        "Updates missed during disconnection are not replayed."
                    )
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.warning("Unable to restore accepted BOA subscription")
                if not self.reconnect:
                    return
                await asyncio.sleep(max(delay, 0.1))
                delay = min(max(delay * 2, 0.1), self.reconnect_max_delay_seconds)

    async def _handle_batch(self, payload) -> None:
        callback = self._callback
        if callback is None or self._stop_requested.is_set():
            return
        try:
            batch = AcceptedBoaPush.from_payload(payload)
        except (KeyError, TypeError, ValueError):
            self.logger.warning("Ignoring malformed accepted BOA batch")
            return
        if not any(
            (batch.accepted_bids, batch.accepted_offers, batch.removed_accepted_bids, batch.removed_accepted_offers)
        ):
            return
        if self._queue is None:
            self._queue = asyncio.Queue()
            if self.callback_queue_maxsize:
                self._callback_slots = asyncio.Semaphore(self.callback_queue_maxsize)
            self._callback_task = asyncio.create_task(self._drain_callbacks())
        if self._callback_slots is not None:
            await self._callback_slots.acquire()
        self._queue.put_nowait((callback, batch))

    async def _drain_callbacks(self) -> None:
        while True:
            callback, batch = await self._queue.get()
            try:
                if inspect.iscoroutinefunction(callback):
                    await callback(batch)
                else:
                    if self._callback_executor is None:
                        self._callback_executor = ThreadPoolExecutor(max_workers=1)
                    result = await asyncio.get_running_loop().run_in_executor(self._callback_executor, callback, batch)
                    if inspect.isawaitable(result):
                        await result
            except Exception:
                self.logger.warning("Error in accepted BOA callback")
            finally:
                self._queue.task_done()
                if self._callback_slots is not None:
                    self._callback_slots.release()

    def unsubscribe_from_accepted_boa_updates(self, *, timeout: float = 30) -> bool:
        """Leave the feed; callbacks already queued may still run."""
        if self._is_running_on_helper_loop():
            raise RuntimeError("Use async_unsubscribe_from_accepted_boa_updates on the helper's event loop")
        if self._loop is None or self._closed:
            return False
        return self._run_in_loop(self._unsubscribe(timeout)).result(timeout=timeout + 5)

    async def async_unsubscribe_from_accepted_boa_updates(self, *, timeout: float = 30) -> bool:
        return await asyncio.to_thread(self.unsubscribe_from_accepted_boa_updates, timeout=timeout)

    async def _unsubscribe(self, timeout) -> bool:
        async with self._get_subscription_lock():
            if not self._subscribed:
                return False
            self._subscribed = False
            await self._stop_heartbeat_monitor()
            self._callback = None
            await self._leave_signalr_group(ACCEPTED_BOA_PUSH_NAME, timeout=timeout)
            self.logger.info("Stopped accepted BOA subscription.")
            return True

    async def _shutdown_async(self, *, wait_for_callbacks: bool, leave_groups_timeout: float = 5) -> None:
        await self._stop_heartbeat_monitor()
        if self._restore_task is not None:
            self._restore_task.cancel()
            await asyncio.gather(self._restore_task, return_exceptions=True)
        await self._unsubscribe(leave_groups_timeout)
        await self._close_hub_connection()
        if self._connection_task is not None:
            self._connection_task.cancel()
            await asyncio.gather(self._connection_task, return_exceptions=True)
        if self._callback_task is not None:
            if wait_for_callbacks:
                await self._queue.join()
            self._callback_task.cancel()
            await asyncio.gather(self._callback_task, return_exceptions=True)
        self._connected.clear()

    def close(self, *, wait_for_callbacks: bool = True, timeout: float = 10) -> None:
        """Close the connection and optionally finish callbacks already queued."""
        if self._is_running_on_helper_loop():
            raise RuntimeError("Close the helper from outside its callback loop")
        if self._closed:
            return
        self.logger.info("Closing accepted BOA SignalR connection...")
        self._stop_requested.set()
        drained = False
        try:
            if self._loop is not None and self._loop.is_running():
                shutdown = self._run_in_loop(
                    self._shutdown_async(wait_for_callbacks=wait_for_callbacks, leave_groups_timeout=min(5, timeout))
                )
                try:
                    shutdown.result(timeout=timeout)
                except TimeoutError:
                    shutdown.cancel()
                    raise
            drained = True
        finally:
            if self._loop is not None and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=timeout)
            if self._callback_executor is not None:
                self._callback_executor.shutdown(
                    wait=wait_for_callbacks and drained, cancel_futures=not wait_for_callbacks or not drained
                )
            self._closed = True
        self.logger.info("Accepted BOA SignalR connection closed.")

    async def async_close(self, *, wait_for_callbacks: bool = True, timeout: float = 10) -> None:
        """Close without blocking the caller's event loop; call outside this helper's callbacks."""
        if self._is_running_on_helper_loop():
            raise RuntimeError("Close the helper from outside its callback loop")
        await asyncio.to_thread(self.close, wait_for_callbacks=wait_for_callbacks, timeout=timeout)
