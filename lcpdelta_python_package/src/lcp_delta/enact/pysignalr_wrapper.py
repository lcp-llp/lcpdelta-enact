import asyncio
import threading
from pysignalr.client import SignalRClient

class MySignalrClient:
    def __init__(self, url, access_token_factory):
        self.url = url
        self.access_token_factory = access_token_factory
        self._loop = None
        self._thread = None
        self._hub_ready = threading.Event()
        self.hub_connection = None

    def _start_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self.hub_connection = SignalRClient(self.url, access_token_factory=self.access_token_factory)
        self.hub_connection.on_error(lambda e: print("SignalR error:", e))
        self._hub_ready.set()
        self._loop.run_forever()

    def start(self):
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()
        # wait until hub is ready
        self._hub_ready.wait()

    def stop(self):
        if self._loop and self.hub_connection:
            asyncio.run_coroutine_threadsafe(self.hub_connection.stop(), self._loop)
        if self._thread:
            self._thread.join(timeout=5)

    def send(self, method, args, on_invocation=None):
        if not self._loop:
            raise RuntimeError("Client not started; call .start() first")
        asyncio.run_coroutine_threadsafe(
            self.hub_connection.send(method, args, on_invocation),
            self._loop
        )

    async def send_async(self, method, args, on_invocation=None):      
        fut = asyncio.get_running_loop().create_future()
        def callback(result):
            fut.set_result(result)
        self.send(method, args, callback)
        return await fut

    def on(self, event_name, handler):
        self._hub_ready.wait()
        self.hub_connection.on(event_name, handler)

    def off(self, event_name):
        self._hub_ready.wait()
        self.hub_connection.off(event_name)

    def on_open(self, callback):
        self._hub_ready.wait()
        self.hub_connection.on_open(callback)

    def on_close(self, callback):
        self._hub_ready.wait()
        self.hub_connection.on_close(callback)
