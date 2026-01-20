from signalrcore.hub_connection_builder import HubConnectionBuilder

class SignalrClient:
    def __init__(self, url, access_token_factory):
        self.hub_connection = (
            HubConnectionBuilder()
            .with_url(
                url,
                options={"access_token_factory": access_token_factory},
            )
            .build()
        )

        
        self.hub_connection.on_error(lambda e: print("SignalR error:", e))
        self.hub_connection.on_reconnect(lambda: print("reconnected!"))

    def start(self):
        self.hub_connection.start()
        self.transport = self.hub_connection.transport
        return True

    def stop(self):
        self.hub_connection.stop()

    def is_running(self):
        return (
            self.hub_connection.transport
            and self.hub_connection.transport.is_running()
        )

    def send(self, method, args, on_invocation):
        self.hub_connection.send(method, args, on_invocation)

    def on(self, event_name, handler):
        self.hub_connection.on(event_name, handler)

    def off(self, event_name):
        self.hub_connection.off(event_name)

    def on_open(self, callback):
        self.hub_connection.on_open(callback)

    def on_close(self, callback):
        self.hub_connection.on_close(callback)

    

    
