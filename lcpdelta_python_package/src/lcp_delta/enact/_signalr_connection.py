from __future__ import annotations

import asyncio
import inspect
import threading
from typing import Any, Callable, Self

from pysignalr.client import SignalRClient

from lcp_delta.enact.api_helper import APIHelper
from lcp_delta.common.http.exceptions import EnactApiError


class _SignalRConnection:
    """Shared connection lifecycle; subclasses own subscriptions and callback dispatch."""

    def connect(self, timeout: float = 10) -> Self:
        """Start the background SignalR connection if it is not already running.

        This method is idempotent while the helper is active, so repeated calls
        do not create additional hub connections.
        """
        if self._closed:
            raise RuntimeError("This DPS helper has been closed. Create a new helper to reconnect.")

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

    async def async_connect(self, timeout: float = 10) -> Self:
        """Start the SignalR connection without blocking the caller's event loop."""
        if self._closed:
            raise RuntimeError("This DPS helper has been closed. Create a new helper to reconnect.")

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
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()

    def _is_running_on_helper_loop(self) -> bool:
        """Return true when the caller is already executing on the helper loop."""
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def _run_in_loop(self, coro):
        """Submit a coroutine to the helper's private event loop from any thread."""
        if self._loop is None:
            raise RuntimeError("Event loop not ready")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    async def _connection_supervisor(self) -> None:
        """Run the SignalR client and recreate it after unexpected disconnects."""
        delay = self.reconnect_initial_delay_seconds

        while not self._stop_requested.is_set():
            try:
                hub_connection = self._build_hub_connection()
                self._client_initialised.set()
                await hub_connection.run()
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

    def _build_hub_connection(self) -> SignalRClient:
        """Create a fresh SignalR client wired with auth and lifecycle handlers."""
        api_helper = self._ensure_api_helper()
        access_token_factory, headers = self._build_access_token_factory()

        self.hub_connection = SignalRClient(
            api_helper.endpoints.DPS,
            access_token_factory=access_token_factory,
            headers=headers,
        )
        self._handlers_registered_for_client = set()

        async def on_error(error) -> None:
            self.logger.warning("SignalR error: %s", error)

        self.hub_connection.on_open(self._on_open)
        self.hub_connection.on_close(self._on_close)
        self.hub_connection.on_error(on_error)
        return self.hub_connection

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
        credentials = self._ensure_api_helper().credentials_holder
        first_token = credentials.bearer_token or self._fetch_bearer_token()
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
        except asyncio.CancelledError:
            self._remove_pending_invocation_handler(on_response)
            raise
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

    async def _leave_signalr_group(self, group_name: str, *, timeout: float) -> None:
        """Best-effort backend `LeaveGroup` call for one SignalR group."""
        if self.hub_connection is None or not self.is_connected:
            return

        try:
            await self._send_with_response("LeaveGroup", [group_name], timeout=timeout)
        except Exception:
            self.logger.warning("Failed to leave DPS group %s", group_name, exc_info=True)

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
