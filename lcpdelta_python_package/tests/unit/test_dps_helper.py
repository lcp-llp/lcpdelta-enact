import asyncio

from lcp_delta.common.http.exceptions import EnactApiError
from lcp_delta.enact.dps_helper import DPSHelper


class _FakeHubConnection:
    def __init__(self):
        self.handlers = {}

    def on(self, push_name, handler):
        self.handlers[push_name] = handler


class _FakeCredentials:
    def __init__(self):
        self.bearer_token = "initial-token"
        self.refresh_count = 0

    def get_bearer_token(self):
        self.refresh_count += 1
        self.bearer_token = f"refreshed-token-{self.refresh_count}"


def _helper_with_fake_hub():
    helper = object.__new__(DPSHelper)
    helper.hub_connection = _FakeHubConnection()
    helper._single_series_subscriptions = []
    helper._multi_series_subscriptions = []
    return helper


def test_access_token_factory_reuses_existing_initial_token():
    helper = object.__new__(DPSHelper)
    credentials = _FakeCredentials()
    helper.enact_credentials = credentials

    access_token_factory, headers = helper._build_access_token_factory()

    assert credentials.refresh_count == 0
    assert headers == {"Authorization": "Bearer initial-token"}
    assert access_token_factory() == "initial-token"
    assert credentials.refresh_count == 0
    assert access_token_factory() == "refreshed-token-1"
    assert credentials.refresh_count == 1


def test_single_series_join_response_raises_signalr_error_messages():
    helper = _helper_with_fake_hub()
    response = {"messages": [{"errorCode": "RateLimited", "message": "Requests to JoinEnactPush are rate limited."}]}

    try:
        asyncio.run(helper._callback_received(response, "subscription-id"))
    except EnactApiError as exc:
        assert exc.error_code == "RateLimited"
        assert exc.message == "Requests to JoinEnactPush are rate limited."
    else:
        raise AssertionError("Expected EnactApiError")


def test_single_series_join_response_allows_message_only_entries_with_push_name():
    helper = _helper_with_fake_hub()
    response = {
        "messages": [{"message": "Informational message"}],
        "data": {"pushName": "Gb&RealtimeDemand&none"},
    }

    asyncio.run(helper._callback_received(response, "subscription-id"))

    assert "Gb&RealtimeDemand&none" in helper.hub_connection.handlers


def test_multi_series_join_response_still_raises_signalr_error_messages():
    helper = _helper_with_fake_hub()
    response = {"messages": [{"errorCode": "DataCapped", "message": "Reached maximum quota of 1000."}]}

    try:
        asyncio.run(helper._callback_received_multi_series(response, lambda _data: None, parse_datetimes=True))
    except EnactApiError as exc:
        assert exc.error_code == "DataCapped"
        assert exc.message == "Reached maximum quota of 1000."
    else:
        raise AssertionError("Expected EnactApiError")
