import asyncio
import json
import logging
import threading
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import lcp_delta.enact.accepted_boa_dps_helper as boa_module

from lcp_delta.common.http.exceptions import EnactApiError
from lcp_delta.enact import AcceptedBoaDPSHelper, AcceptedBoaPush
from lcp_delta.enact.accepted_boa_dps_helper import ACCEPTED_BOA_PUSH_NAME


def payload(revision=105):
    return {
        "date": "2026-11-06",
        "period": 24,
        "revision": revision,
        "acceptedBids": [{"deltaId": "bid-1", "isBid": True, "volume": -12.5, "sel": None}],
        "acceptedOffers": [{"deltaId": "offer-1", "isBid": False, "volume": 20.0}],
        "removedAcceptedBids": [{"deltaId": "old-bid", "isBid": True, "volume": -10.0}],
        "removedAcceptedOffers": [],
    }


def test_backend_serialization_fixture_decodes_without_losing_public_fields():
    raw = json.loads((Path(__file__).parent / "fixtures/accepted-boa-push.json").read_text())
    batch = AcceptedBoaPush.from_payload(raw)
    assert batch.date == date(2026, 11, 6)
    assert batch.revision == 105
    assert batch.accepted_bids[0] == raw["acceptedBids"][0]
    assert batch.accepted_bids[0]["deltaId"] == "EXAMPLE_PLANT_-1_False_False_False_False_100_50"
    assert batch.accepted_bids[0]["acceptedTime"] is None


class FakeHub:
    def __init__(self):
        self.handlers = {}
        self.calls = []
        self.responses = {}
        self.closed = False

    def on(self, name, handler):
        assert name not in self.handlers
        self.handlers[name] = handler

    async def send(self, method, args, callback):
        self.calls.append((method, args))
        await callback(self.responses.get(method, {"data": {"pushName": ACCEPTED_BOA_PUSH_NAME}}))

    async def close(self):
        self.closed = True


def helper(callback=lambda _batch: None, **kwargs):
    stream = AcceptedBoaDPSHelper("user", "key", callback, auto_connect=False, **kwargs)
    stream.hub_connection = FakeHub()
    stream._connected.set()
    return stream


def test_decodes_complete_batch_without_changing_values_or_aliasing_input():
    raw = payload()
    raw["acceptedBids"][0]["bidPrice"] = Decimal("51.234567")
    batch = AcceptedBoaPush.from_payload([raw])
    assert batch.date == date(2026, 11, 6)
    assert (batch.period, batch.revision) == (24, 105)
    assert batch.accepted_bids[0]["bidPrice"] == Decimal("51.234567")
    assert batch.accepted_bids[0]["sel"] is None
    assert batch.accepted_offers[0]["deltaId"] == "offer-1"
    assert batch.removed_accepted_bids[0]["deltaId"] == "old-bid"
    assert batch.removed_accepted_offers == ()
    raw["acceptedBids"][0]["volume"] = 999
    assert batch.accepted_bids[0]["volume"] == -12.5


@pytest.mark.parametrize(
    "change",
    [
        {"date": "invalid"},
        {"period": True},
        {"period": 51},
        {"revision": -1},
        {"revision": True},
        {"acceptedBids": [{}]},
        {"acceptedOffers": None},
        {"removedAcceptedBids": "invalid"},
    ],
)
def test_rejects_malformed_contract(change):
    raw = payload()
    raw.update(change)
    with pytest.raises((ValueError, KeyError, TypeError)):
        AcceptedBoaPush.from_payload(raw)


def test_subscription_reuses_group_and_updates_callback_without_duplicate_handlers():
    async def run():
        stream = helper()
        first = lambda _batch: None
        second = lambda _batch: None
        assert await stream._subscribe(first, 1) == ACCEPTED_BOA_PUSH_NAME
        assert await stream._subscribe(second, 1) == ACCEPTED_BOA_PUSH_NAME
        assert stream._callback is second
        assert stream.group_names == (ACCEPTED_BOA_PUSH_NAME,)
        assert stream.hub_connection.calls == [("JoinAcceptedBoaPush", [])]
        assert list(stream.hub_connection.handlers) == [
            ACCEPTED_BOA_PUSH_NAME,
            "heartbeat",
            "zeroPnUpdate",
            "earliestDateInBuffer",
        ]
        assert await stream._unsubscribe(1)
        assert not await stream._unsubscribe(1)
        assert stream.hub_connection.calls[-1] == ("LeaveGroup", [ACCEPTED_BOA_PUSH_NAME])
        assert stream.group_names == ()

    asyncio.run(run())


def test_join_errors_keep_structured_fields():
    async def run():
        stream = helper()
        stream.hub_connection.responses["JoinAcceptedBoaPush"] = {
            "messages": [{"errorCode": "DataCapped", "message": "Allowance exhausted"}]
        }
        with pytest.raises(EnactApiError) as error:
            await stream._subscribe(lambda _batch: None, 1)
        assert error.value.error_code == "DataCapped"
        assert error.value.message == "Allowance exhausted"
        assert not stream._subscribed

    asyncio.run(run())


def test_reconnect_restores_future_delivery_and_rejoins_expired_subscription():
    async def run():
        stream = helper()
        stream._subscribed = True
        await stream._restore_subscription()
        assert stream.hub_connection.calls == [("ReconnectToPush", [ACCEPTED_BOA_PUSH_NAME])]
        stream.hub_connection.responses["ReconnectToPush"] = {
            "messages": [{"errorCode": "DataFormatError", "message": "Subscribe again"}]
        }
        await stream._restore_subscription()
        assert stream.hub_connection.calls[-1] == ("JoinAcceptedBoaPush", [])

    asyncio.run(run())


def test_unauthorized_reconnect_does_not_fall_back_to_join():
    async def run():
        stream = helper(reconnect=False)
        stream._subscribed = True
        stream.hub_connection.responses["ReconnectToPush"] = {
            "messages": [{"errorCode": "InsufficientAccessPrivileges", "message": "No access"}]
        }
        await stream._restore_subscription()
        assert [method for method, _ in stream.hub_connection.calls] == ["ReconnectToPush"]

    asyncio.run(run())


def test_sync_callbacks_receive_whole_batches_in_order_without_revision_dedup_or_fetch():
    async def run():
        batches = []
        threads = []

        def callback(batch):
            batches.append(batch)
            threads.append(threading.get_ident())

        stream = helper(callback)
        # No API helper is constructed or HTTP snapshot requested for push processing.
        for revision in [105, 105, 109, 1]:
            await stream._handle_batch([payload(revision)])
        await stream._queue.join()
        assert [batch.revision for batch in batches] == [105, 105, 109, 1]
        assert all(len(batch.accepted_bids) + len(batch.accepted_offers) == 2 for batch in batches)
        assert all(thread != threading.get_ident() for thread in threads)
        assert stream.api_helper is None
        await stream._shutdown_async(wait_for_callbacks=True)
        stream._callback_executor.shutdown()

    asyncio.run(run())


def test_bounded_callbacks_apply_backpressure_and_continue_after_callback_error():
    async def run():
        release = asyncio.Event()
        started = asyncio.Event()
        received = []

        async def callback(batch):
            started.set()
            await release.wait()
            received.append(batch.revision)
            if batch.revision == 1:
                raise RuntimeError("failed callback")

        stream = helper(callback, callback_queue_maxsize=1)
        await stream._handle_batch(payload(1))
        await asyncio.wait_for(started.wait(), 1)
        waiting = asyncio.create_task(stream._handle_batch(payload(2)))
        await asyncio.sleep(0)
        assert not waiting.done()
        release.set()
        await asyncio.wait_for(waiting, 1)
        await asyncio.wait_for(stream._queue.join(), 1)
        assert received == [1, 2]
        await stream._shutdown_async(wait_for_callbacks=True)
        assert stream.hub_connection.closed

    asyncio.run(run())


def test_malformed_and_empty_batches_are_ignored_without_logging_customer_data(caplog):
    async def run():
        stream = helper()
        await stream._handle_batch({"sensitive": "customer-value"})
        empty = payload()
        for key in ["acceptedBids", "acceptedOffers", "removedAcceptedBids", "removedAcceptedOffers"]:
            empty[key] = []
        await stream._handle_batch(empty)
        assert stream._queue is None
        assert "customer-value" not in caplog.text

    asyncio.run(run())


def test_public_sync_async_subscription_and_close_parity_without_network():
    stream = helper()
    stream.connect = lambda **_kwargs: stream
    stream._start_loop_thread()
    try:
        assert stream.subscribe_to_accepted_boa_updates() == ACCEPTED_BOA_PUSH_NAME
        assert asyncio.run(stream.async_subscribe_to_accepted_boa_updates()) == ACCEPTED_BOA_PUSH_NAME
        assert asyncio.run(stream.async_unsubscribe_from_accepted_boa_updates())
        assert stream.unsubscribe_from_accepted_boa_updates() is False
        assert stream.subscribe_to_accepted_boa_updates() == ACCEPTED_BOA_PUSH_NAME
        asyncio.run(stream.async_close())
        assert stream._closed
        assert not stream._thread.is_alive()
        assert stream.hub_connection.closed
        stream.close()
    finally:
        if not stream._closed:
            stream.close()


def test_shutdown_drains_queued_batches_before_returning():
    received = []
    stream = helper(lambda batch: received.append(batch.revision))
    stream._start_loop_thread()
    stream._run_in_loop(stream._handle_batch(payload(1))).result(timeout=2)
    stream._run_in_loop(stream._handle_batch(payload(2))).result(timeout=2)
    stream.close(wait_for_callbacks=True)
    assert received == [1, 2]
    assert not stream.is_connected


def test_auth_token_factory_is_shared_and_refreshes_for_later_connections():
    class Credentials:
        bearer_token = "first"

        def get_bearer_token(self):
            self.bearer_token = "refreshed"

    class Api:
        credentials_holder = Credentials()

    stream = helper()
    stream.api_helper = Api()
    token_factory, headers = stream._build_access_token_factory()
    assert headers == {"Authorization": "Bearer first"}
    assert token_factory() == "first"
    assert token_factory() == "refreshed"


def test_connection_supervisor_retries_initial_auth_failure():
    async def run():
        stream = helper(reconnect_initial_delay_seconds=0)
        attempts = []

        class Connection:
            async def run(self):
                stream._stop_requested.set()

        def build():
            attempts.append(1)
            if len(attempts) == 1:
                raise OSError("Authentication unavailable")
            return Connection()

        stream._build_hub_connection = build
        await stream._connection_supervisor()
        assert len(attempts) == 2
        assert stream._client_initialised.is_set()

    asyncio.run(run())


def test_shutdown_timeout_cancels_async_callbacks_and_stops_helper_thread():
    stream = helper()
    started = threading.Event()

    async def blocked(_batch):
        started.set()
        await asyncio.Event().wait()

    stream._callback = blocked
    stream._start_loop_thread()
    stream._run_in_loop(stream._handle_batch(payload())).result(timeout=2)
    assert started.wait(2)
    with pytest.raises(TimeoutError):
        stream.close(timeout=0.2)
    assert stream._closed
    assert not stream._thread.is_alive()


def test_backpressured_batch_keeps_its_callback_when_unsubscribing():
    async def run():
        release = asyncio.Event()
        started = asyncio.Event()
        received = []

        async def callback(batch):
            started.set()
            await release.wait()
            received.append(batch.revision)

        stream = helper(callback, callback_queue_maxsize=1)
        await stream._subscribe(callback, 1)
        await stream._handle_batch(payload(1))
        await asyncio.wait_for(started.wait(), 1)
        waiting = asyncio.create_task(stream._handle_batch(payload(2)))
        await asyncio.sleep(0)
        assert not waiting.done()
        await stream._unsubscribe(1)
        release.set()
        await asyncio.wait_for(waiting, 1)
        await asyncio.wait_for(stream._queue.join(), 1)
        assert received == [1, 2]
        await stream._shutdown_async(wait_for_callbacks=True)

    asyncio.run(run())


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), -float("inf")])
def test_heartbeat_timeout_requires_positive_finite_value(timeout):
    with pytest.raises(ValueError, match="heartbeat_timeout_seconds"):
        helper(heartbeat_timeout_seconds=timeout)


def test_heartbeat_warns_once_per_outage_and_logs_recovery_using_local_receipt_time(monkeypatch, caplog):
    clock = [100.0]
    monkeypatch.setattr(boa_module, "monotonic", lambda: clock[0])
    caplog.set_level(logging.INFO)

    async def run():
        stream = helper()
        await stream._subscribe(lambda _batch: pytest.fail("Heartbeat reached BOA callback"), 1)
        clock[0] = 129.9
        stream._check_heartbeat()
        assert not caplog.records
        clock[0] = 130
        stream._check_heartbeat()
        clock[0] = 180
        stream._check_heartbeat()
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.WARNING
        assert "live updates may be stale" in caplog.text
        assert stream.is_connected

        # The server timestamp can be invalid or skewed; only receipt time matters.
        await stream.hub_connection.handlers["heartbeat"](["invalid-server-timestamp"])
        await stream.hub_connection.handlers["heartbeat"](["different-server-clock"])
        assert len(caplog.records) == 2
        assert caplog.records[1].levelno == logging.INFO
        assert "resumed" in caplog.records[1].getMessage()
        clock[0] = 209.9
        stream._check_heartbeat()
        assert len(caplog.records) == 2
        clock[0] = 210
        for name in ("zeroPnUpdate", "earliestDateInBuffer"):
            await stream.hub_connection.handlers[name]([])
        assert stream._last_heartbeat_monotonic == 180
        stream._check_heartbeat()
        assert len(caplog.records) == 3
        assert stream._queue is None
        assert stream.hub_connection.calls == [("JoinAcceptedBoaPush", [])]
        await stream._shutdown_async(wait_for_callbacks=True)

    asyncio.run(run())


def test_heartbeat_monitor_pauses_on_disconnect_and_resets_grace_on_reconnect(monkeypatch, caplog):
    clock = [100.0]
    monkeypatch.setattr(boa_module, "monotonic", lambda: clock[0])

    async def run():
        stream = helper()
        await stream._subscribe(lambda _batch: None, 1)
        first_monitor = stream._heartbeat_task
        clock[0] = 130
        stream._check_heartbeat()
        assert len(caplog.records) == 1
        await stream._on_close()
        assert first_monitor.cancelled()
        clock[0] = 1000
        stream._check_heartbeat()
        assert len(caplog.records) == 1
        stream.hub_connection = FakeHub()
        stream._handlers_registered_for_client = set()
        await stream._on_open()
        clock[0] = 1029.9
        stream._check_heartbeat()
        assert len(caplog.records) == 1
        clock[0] = 1030
        stream._check_heartbeat()
        assert len(caplog.records) == 2
        await stream._shutdown_async(wait_for_callbacks=True)
        assert stream._heartbeat_task is None

    asyncio.run(run())


def test_unsubscribe_and_shutdown_cancel_heartbeat_monitor_without_false_warnings(monkeypatch, caplog):
    clock = [100.0]
    monkeypatch.setattr(boa_module, "monotonic", lambda: clock[0])

    async def run():
        stream = helper()
        callback = lambda _batch: None
        await stream._subscribe(callback, 1)
        first_monitor = stream._heartbeat_task
        await stream._unsubscribe(1)
        clock[0] = 1000
        stream._check_heartbeat()
        assert first_monitor.cancelled()
        assert stream._heartbeat_task is None
        assert not caplog.records
        await stream._subscribe(callback, 1)
        second_monitor = stream._heartbeat_task
        assert second_monitor is not first_monitor
        clock[0] = 2000
        stream._stop_requested.set()
        stream._check_heartbeat()
        assert not caplog.records
        await stream._shutdown_async(wait_for_callbacks=True)
        assert second_monitor.cancelled()
        assert stream._heartbeat_task is None

    asyncio.run(run())


def test_heartbeat_monitor_can_be_disabled(monkeypatch, caplog):
    clock = [100.0]
    monkeypatch.setattr(boa_module, "monotonic", lambda: clock[0])

    async def run():
        stream = helper(heartbeat_timeout_seconds=None)
        await stream._subscribe(lambda _batch: None, 1)
        assert stream._heartbeat_task is None
        clock[0] = 10000
        stream._check_heartbeat()
        await stream._handle_heartbeat([])
        assert not caplog.records
        await stream._shutdown_async(wait_for_callbacks=True)

    asyncio.run(run())


def test_heartbeat_watchdog_task_checks_freshness_without_a_heartbeat(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(boa_module, "monotonic", lambda: clock[0])

    async def run():
        warning = asyncio.Event()

        class CaptureWarning(logging.Handler):
            def emit(self, record):
                if "Enact heartbeat overdue" in record.getMessage():
                    warning.set()

        logger = logging.Logger("heartbeat-watchdog-test")
        logger.addHandler(CaptureWarning())
        stream = helper(heartbeat_timeout_seconds=0.01, logger=logger)
        await stream._subscribe(lambda _batch: None, 1)
        clock[0] = 101
        try:
            await asyncio.wait_for(warning.wait(), 1)
            assert stream._heartbeat_overdue
            assert stream.is_connected
        finally:
            await stream._shutdown_async(wait_for_callbacks=True)

    asyncio.run(run())
