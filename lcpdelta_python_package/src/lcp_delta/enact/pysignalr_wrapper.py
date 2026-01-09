import threading
import asyncio
from pysignalr.client import SignalRClient as PySignalRClient

class SignalrClient:
    def __init__(self, url, access_token_factory):
        self.url = url
        self.access_token_factory = access_token_factory

        self._on_open_callback = None
        self._on_close_callback = None

        self.hub_connection = PySignalRClient(url, access_token_factory=access_token_factory)

        self.hub_connection.on_error(lambda e: print("SignalR error:", e))

        # Event loop and background thread
        self._loop = None
        self._thread = None

    def _run_loop(self):
        """Background thread that runs the event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        # Run forever
        try:
            self._loop.run_until_complete(self.hub_connection.start())
        except Exception as e:
            print(f"Error in event loop: {e}")

    def start(self):
        """Start SignalR client in a background thread."""
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        import time
        time.sleep(1)

        return True

    def stop(self):
        if self._loop:
            asyncio.run_coroutine_threadsafe(self.hub_connection.stop(), self._loop)
        if self._thread:
            self._thread.join(timeout=5)

    def is_running(self):
        # pysignalr client has 'state', 1 = connected
        return getattr(self.hub_connection, "state", 0) == 1

    def send(self, method, args, on_invocation):
        # Run send in the loop
        if self._loop:
            asyncio.run_coroutine_threadsafe(
                self.hub_connection.send(method, args, on_invocation),
                self._loop
            )

    def on(self, event_name, handler):
        self.hub_connection.on(event_name, handler)

    def off(self, event_name):
        self.hub_connection.off(event_name)

    def on_open(self, callback):
        self._on_open_callback = callback
        self.hub_connection.on_open(callback)

    def on_close(self, callback):
        self._on_close_callback = callback
        self.hub_connection.on_close(callback)
