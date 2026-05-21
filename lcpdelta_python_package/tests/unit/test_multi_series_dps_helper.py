from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from lcp_delta.common.http.exceptions import EnactApiError
from lcp_delta.enact.multi_series_dps_helper import MultiSeriesDPSHelper, MultiSeriesPushMetadata


def test_public_enact_package_exports_multi_series_helper():
    from lcp_delta import enact
    from lcp_delta.enact import MultiSeriesDPSHelper as PublicMultiSeriesDPSHelper

    assert enact.MultiSeriesDPSHelper is MultiSeriesDPSHelper
    assert PublicMultiSeriesDPSHelper is MultiSeriesDPSHelper


class _CapturingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


async def _wait_for_callback_processing(helper):
    for queue in helper._callback_queues:
        await queue.join()
    for task in helper._callback_tasks:
        task.cancel()
    await asyncio.gather(*helper._callback_tasks, return_exceptions=True)


def _test_push_metadata(sequence=1):
    return MultiSeriesPushMetadata(
        sequence=sequence,
        received_at_utc=datetime.now(timezone.utc),
        group_name="multi-series-group",
        push_id="Gb&RealtimeDemand&none",
        series_id="RealtimeDemand",
        country_id="Gb",
    )


def _test_subscription(callback):
    return type(
        "Subscription",
        (),
        {"join_payload": [], "callback": callback, "parse_datetimes": True},
    )()


def test_unhandled_signalr_server_method_warning_is_suppressed_without_hiding_real_warnings():
    MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    signalr_logger = logging.getLogger("pysignalr.client")
    handler = _CapturingHandler()
    previous_level = signalr_logger.level
    previous_propagate = signalr_logger.propagate
    previous_disabled = signalr_logger.disabled

    signalr_logger.addHandler(handler)
    signalr_logger.setLevel(logging.WARNING)
    signalr_logger.propagate = False
    signalr_logger.disabled = False
    try:
        signalr_logger.warning("No client method with the name '%s' found.", "zeroPnUpdate")
        signalr_logger.warning("SignalR connection stopped unexpectedly: %s", "closed")
    finally:
        signalr_logger.removeHandler(handler)
        signalr_logger.setLevel(previous_level)
        signalr_logger.propagate = previous_propagate
        signalr_logger.disabled = previous_disabled

    assert handler.messages == ["SignalR connection stopped unexpectedly: closed"]


def test_series_ping_payload_is_converted_to_dataframe_and_metadata():
    payload = [
        {
            "refresh": False,
            "plantMeta": {
                "plantId": "T_UNIT-1",
                "owner": "OwnerCo",
                "fuel": "CCGT",
                "zone": "Z1",
            },
            "data": {
                "day": "19052026",
                "id": "Gb&RollingBidsAccepted&T_UNIT-1",
                "replaceSeries": False,
                "data": [
                    {
                        "deletePoint": False,
                        "newPoint": True,
                        "byPoint": False,
                        "current": {
                            "arrayPoint": [1789718400000, 12.5],
                            "datePeriod": {"datePeriodCombinedGmt": "2026-09-18T08:00:00"},
                        },
                    }
                ],
            },
        }
    ]

    frame, metadata = MultiSeriesDPSHelper._series_ping_to_frame_and_metadata(
        payload,
        sequence=7,
        group_name="multi-series-group",
        parse_datetimes=True,
    )

    assert metadata.sequence == 7
    assert metadata.country_id == "Gb"
    assert metadata.series_id == "RollingBidsAccepted"
    assert metadata.option_ids == ("T_UNIT-1",)
    assert metadata.day == "19052026"
    assert metadata.plant.plant_id == "T_UNIT-1"
    assert metadata.plant.owner == "OwnerCo"
    assert metadata.plant.fuel == "CCGT"
    assert metadata.plant.zone == "Z1"

    assert frame.index[0] == pd.Timestamp("2026-09-18T08:00:00Z")
    assert frame.loc[pd.Timestamp("2026-09-18T08:00:00Z"), "Gb&RollingBidsAccepted&T_UNIT-1"] == 12.5
    assert frame.attrs["enact_push_metadata"] == metadata


def test_series_ping_multi_option_id_is_split_into_option_metadata():
    frame, metadata = MultiSeriesDPSHelper._series_ping_to_frame_and_metadata(
        {
            "data": {
                "id": "Belgium&AFRRVolumeRealtime&Up*Median",
                "data": [
                    {
                        "current": {
                            "arrayPoint": [1789718400000, 4.2],
                        },
                    }
                ],
            }
        },
        sequence=1,
        group_name="multi-series-group",
        parse_datetimes=False,
    )

    assert metadata.country_id == "Belgium"
    assert metadata.series_id == "AFRRVolumeRealtime"
    assert metadata.option_ids == ("Up", "Median")
    assert frame.index[0] == "2026-09-18T08:00:00Z"


def test_series_ping_trims_trailing_null_multi_value_columns():
    frame, _metadata = MultiSeriesDPSHelper._series_ping_to_frame_and_metadata(
        {
            "data": {
                "id": "Gb&PredictedSystemPrice&P50",
                "data": [
                    {
                        "current": {
                            "arrayPoint": [1779379200000, 118.37, 131.98, None, None],
                            "datePeriod": {"datePeriodCombinedGmt": "2026-05-21T16:00:00Z"},
                        },
                    },
                    {
                        "current": {
                            "arrayPoint": [1779381000000, 115.0, 119.9, None, None],
                            "datePeriod": {"datePeriodCombinedGmt": "2026-05-21T16:30:00Z"},
                        },
                    },
                ],
            }
        },
        sequence=1,
        group_name="multi-series-group",
        parse_datetimes=True,
    )

    assert list(frame.columns) == [
        "Gb&PredictedSystemPrice&P50_0",
        "Gb&PredictedSystemPrice&P50_1",
    ]
    assert frame.loc[pd.Timestamp("2026-05-21T16:00:00Z"), "Gb&PredictedSystemPrice&P50_0"] == 118.37
    assert frame.loc[pd.Timestamp("2026-05-21T16:00:00Z"), "Gb&PredictedSystemPrice&P50_1"] == 131.98


def test_series_ping_preserves_middle_null_multi_value_columns():
    frame, _metadata = MultiSeriesDPSHelper._series_ping_to_frame_and_metadata(
        {
            "data": {
                "id": "Gb&PredictedSystemPrice&P50",
                "data": [
                    {
                        "current": {
                            "arrayPoint": [1779379200000, 118.37, None, 131.98, None],
                            "datePeriod": {"datePeriodCombinedGmt": "2026-05-21T16:00:00Z"},
                        },
                    },
                ],
            }
        },
        sequence=1,
        group_name="multi-series-group",
        parse_datetimes=True,
    )

    assert list(frame.columns) == [
        "Gb&PredictedSystemPrice&P50_0",
        "Gb&PredictedSystemPrice&P50_1",
        "Gb&PredictedSystemPrice&P50_2",
    ]
    assert pd.isna(frame.loc[pd.Timestamp("2026-05-21T16:00:00Z"), "Gb&PredictedSystemPrice&P50_1"])
    assert frame.loc[pd.Timestamp("2026-05-21T16:00:00Z"), "Gb&PredictedSystemPrice&P50_2"] == 131.98


def test_series_ping_uses_consistent_multi_value_columns_after_trimming():
    frame, _metadata = MultiSeriesDPSHelper._series_ping_to_frame_and_metadata(
        {
            "data": {
                "id": "Gb&PredictedSystemPrice&P50",
                "data": [
                    {
                        "current": {
                            "arrayPoint": [1779379200000, 118.37, None, None],
                            "datePeriod": {"datePeriodCombinedGmt": "2026-05-21T16:00:00Z"},
                        },
                    },
                    {
                        "current": {
                            "arrayPoint": [1779381000000, 115.0, 119.9, None],
                            "datePeriod": {"datePeriodCombinedGmt": "2026-05-21T16:30:00Z"},
                        },
                    },
                ],
            }
        },
        sequence=1,
        group_name="multi-series-group",
        parse_datetimes=True,
    )

    assert list(frame.columns) == [
        "Gb&PredictedSystemPrice&P50_0",
        "Gb&PredictedSystemPrice&P50_1",
    ]
    assert frame.loc[pd.Timestamp("2026-05-21T16:00:00Z"), "Gb&PredictedSystemPrice&P50_0"] == 118.37
    assert pd.isna(frame.loc[pd.Timestamp("2026-05-21T16:00:00Z"), "Gb&PredictedSystemPrice&P50_1"])
    assert frame.loc[pd.Timestamp("2026-05-21T16:30:00Z"), "Gb&PredictedSystemPrice&P50_1"] == 119.9


def test_callback_dispatch_is_fifo_when_callbacks_are_sequential():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False, concurrent_callbacks=False)
    seen_sequences = []

    async def callback(_frame, metadata):
        await asyncio.sleep(0)
        seen_sequences.append(metadata.sequence)

    async def run():
        for sequence in (1, 2, 3):
            metadata = MultiSeriesPushMetadata(
                sequence=sequence,
                received_at_utc=datetime.now(timezone.utc),
                group_name="group",
                push_id="Gb&RealtimeDemand&none",
                series_id="RealtimeDemand",
                country_id="Gb",
            )
            await helper._enqueue_callback(callback, pd.DataFrame({"value": [sequence]}), metadata)

        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert seen_sequences == [1, 2, 3]


def test_concurrent_callbacks_start_in_fifo_order_and_respect_worker_limit():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False, concurrent_callbacks=True, max_workers=2)
    first_push_id = "Gb&RealtimeDemand&none"
    second_push_id = next(
        f"Gb&RealtimeFrequency&none-{index}"
        for index in range(20)
        if helper._get_callback_shard_index(first_push_id, 2)
        != helper._get_callback_shard_index(f"Gb&RealtimeFrequency&none-{index}", 2)
    )
    started_by_push_id: dict[str | None, list[int]] = {first_push_id: [], second_push_id: []}
    running_count = 0
    max_running_count = 0

    async def callback(_frame, metadata):
        nonlocal running_count, max_running_count

        started_by_push_id[metadata.push_id].append(metadata.sequence)
        running_count += 1
        max_running_count = max(max_running_count, running_count)
        await asyncio.sleep(0.05)
        running_count -= 1

    async def run():
        for sequence, push_id, series_id in (
            (1, first_push_id, "RealtimeDemand"),
            (2, second_push_id, "RealtimeFrequency"),
            (3, first_push_id, "RealtimeDemand"),
        ):
            metadata = MultiSeriesPushMetadata(
                sequence=sequence,
                received_at_utc=datetime.now(timezone.utc),
                group_name="group",
                push_id=push_id,
                series_id=series_id,
                country_id="Gb",
            )
            await helper._enqueue_callback(callback, pd.DataFrame({"value": [sequence]}), metadata)

        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert started_by_push_id[first_push_id] == [1, 3]
    assert started_by_push_id[second_push_id] == [2]
    assert max_running_count == 2


def test_reconnect_callback_gates_queued_pushes_until_it_completes():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    events = []

    async def callback(_frame, _metadata):
        events.append("push")

    async def fake_reconnect_group(group_name, _subscription):
        events.append(f"restore:{group_name}")

    async def run():
        reconnect_started = asyncio.Event()
        release_reconnect = asyncio.Event()

        async def reconnect_callback(helper_arg):
            assert helper_arg is helper
            events.append("reconnect-start")
            reconnect_started.set()
            await release_reconnect.wait()
            events.append("reconnect-end")

        helper.on_reconnected(reconnect_callback)
        helper._reconnect_group = fake_reconnect_group
        subscription = _test_subscription(callback)
        helper._subscriptions["multi-series-group"] = subscription
        helper._connected.set()
        helper._pause_push_processing()

        restore_task = asyncio.create_task(
            helper._restore_groups_after_open([("multi-series-group", subscription)])
        )
        await reconnect_started.wait()

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [1]}), _test_push_metadata())
        await asyncio.sleep(0)

        assert events == ["restore:multi-series-group", "reconnect-start"]

        release_reconnect.set()
        await restore_task
        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert events == ["restore:multi-series-group", "reconnect-start", "reconnect-end", "push"]


def test_pushes_queued_before_reconnect_pause_continue_to_drain():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    events = []
    release_first_push = None

    async def callback(_frame, metadata):
        events.append(f"start:{metadata.sequence}")
        if metadata.sequence == 1:
            assert release_first_push is not None
            await release_first_push.wait()
        events.append(f"end:{metadata.sequence}")

    async def run():
        nonlocal release_first_push
        release_first_push = asyncio.Event()

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [1]}), _test_push_metadata(sequence=1))
        while events != ["start:1"]:
            await asyncio.sleep(0)

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [2]}), _test_push_metadata(sequence=2))
        helper._pause_push_processing()
        await helper._enqueue_callback(callback, pd.DataFrame({"value": [3]}), _test_push_metadata(sequence=3))

        release_first_push.set()
        for _ in range(20):
            if "end:2" in events:
                break
            await asyncio.sleep(0)

        assert events == ["start:1", "end:1", "start:2", "end:2"]

        helper._release_push_processing()
        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert events == ["start:1", "end:1", "start:2", "end:2", "start:3", "end:3"]


def test_reconnect_callback_waits_for_pre_reconnect_queue_to_drain():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    events = []
    release_first_push = None

    async def callback(_frame, metadata):
        events.append(f"start:{metadata.sequence}")
        if metadata.sequence == 1:
            assert release_first_push is not None
            await release_first_push.wait()
        events.append(f"end:{metadata.sequence}")

    async def fake_reconnect_group(_group_name, _subscription):
        events.append("restore")

    async def reconnect_callback():
        events.append("reconnect")

    async def run():
        nonlocal release_first_push
        release_first_push = asyncio.Event()

        helper.on_reconnected(reconnect_callback)
        helper._reconnect_group = fake_reconnect_group
        subscription = _test_subscription(callback)
        helper._subscriptions["multi-series-group"] = subscription
        helper._connected.set()

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [1]}), _test_push_metadata(sequence=1))
        while events != ["start:1"]:
            await asyncio.sleep(0)

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [2]}), _test_push_metadata(sequence=2))
        pre_reconnect_drained = helper._pause_push_processing()
        restore_task = asyncio.create_task(
            helper._restore_groups_after_open([("multi-series-group", subscription)], pre_reconnect_drained)
        )
        while "restore" not in events:
            await asyncio.sleep(0)

        assert events == ["start:1", "restore"]

        release_first_push.set()
        await restore_task
        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert events == ["start:1", "restore", "end:1", "start:2", "end:2", "reconnect"]


def test_connection_loss_during_restore_retries_without_releasing_reconnect_queue():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    events = []
    release_first_push = None
    restore_attempts = 0

    async def callback(_frame, metadata):
        events.append(f"start:{metadata.sequence}")
        if metadata.sequence == 1:
            assert release_first_push is not None
            await release_first_push.wait()
        events.append(f"end:{metadata.sequence}")

    async def reconnect_callback():
        events.append("reconnect")

    async def fake_reconnect_group(_group_name, _subscription):
        nonlocal restore_attempts
        restore_attempts += 1
        events.append(f"restore:{restore_attempts}")
        if restore_attempts == 1:
            helper._connected.clear()

    async def run():
        nonlocal release_first_push
        release_first_push = asyncio.Event()

        helper.on_reconnected(reconnect_callback)
        helper._reconnect_group = fake_reconnect_group
        subscription = _test_subscription(callback)
        helper._subscriptions["multi-series-group"] = subscription
        helper._connected.set()

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [1]}), _test_push_metadata(sequence=1))
        while events != ["start:1"]:
            await asyncio.sleep(0)

        pre_reconnect_drained = helper._pause_push_processing()
        await helper._enqueue_callback(callback, pd.DataFrame({"value": [2]}), _test_push_metadata(sequence=2))
        await helper._restore_groups_after_open([("multi-series-group", subscription)], pre_reconnect_drained)
        await asyncio.sleep(0)

        assert events == ["start:1", "restore:1"]
        assert not helper._get_push_processing_gate().ready.is_set()

        helper._connected.set()
        retry_pre_reconnect_drained = helper._pause_push_processing()
        assert retry_pre_reconnect_drained is pre_reconnect_drained

        restore_task = asyncio.create_task(
            helper._restore_groups_after_open([("multi-series-group", subscription)], retry_pre_reconnect_drained)
        )
        while "restore:2" not in events:
            await asyncio.sleep(0)

        assert events == ["start:1", "restore:1", "restore:2"]

        release_first_push.set()
        await restore_task
        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert events == ["start:1", "restore:1", "restore:2", "end:1", "reconnect", "start:2", "end:2"]


def test_sync_reconnect_callback_can_accept_helper_argument():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    seen_helpers = []

    def reconnect_callback(helper_arg):
        seen_helpers.append(helper_arg)

    async def run():
        helper.on_reconnected(reconnect_callback)
        helper._connected.set()
        await helper._run_reconnect_callback_with_retries()

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert seen_helpers == [helper]


def test_reconnect_callback_retries_failures_until_success():
    helper = MultiSeriesDPSHelper(
        "username",
        "api-key",
        auto_connect=False,
        reconnect_initial_delay_seconds=0.001,
        reconnect_max_delay_seconds=0.001,
        reconnect_callback_timeout_seconds=1,
    )
    attempts = 0

    async def reconnect_callback():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("snapshot refresh failed")

    async def run():
        helper.on_reconnected(reconnect_callback)
        helper._connected.set()
        await helper._run_reconnect_callback_with_retries()

    asyncio.run(run())

    assert attempts == 2


def test_hanging_reconnect_callback_timeout_releases_queued_pushes():
    helper = MultiSeriesDPSHelper(
        "username",
        "api-key",
        auto_connect=False,
        reconnect_callback_timeout_seconds=0.01,
    )
    events = []

    async def reconnect_callback():
        events.append("reconnect-start")
        await asyncio.Event().wait()

    async def callback(_frame, _metadata):
        events.append("push")

    async def fake_reconnect_group(_group_name, _subscription):
        return None

    async def run():
        helper.on_reconnected(reconnect_callback)
        helper._reconnect_group = fake_reconnect_group
        subscription = _test_subscription(callback)
        helper._subscriptions["multi-series-group"] = subscription
        helper._connected.set()
        helper._pause_push_processing()

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [1]}), _test_push_metadata())
        await helper._restore_groups_after_open([("multi-series-group", subscription)])
        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert events == ["reconnect-start", "push"]


def test_reconnect_callback_timeout_releases_queued_pushes_after_failure():
    helper = MultiSeriesDPSHelper(
        "username",
        "api-key",
        auto_connect=False,
        reconnect_initial_delay_seconds=0,
        reconnect_max_delay_seconds=0,
        reconnect_callback_timeout_seconds=0.001,
    )
    attempts = 0
    events = []

    async def reconnect_callback():
        nonlocal attempts
        attempts += 1
        raise RuntimeError("snapshot refresh still failing")

    async def callback(_frame, _metadata):
        events.append("push")

    async def fake_reconnect_group(_group_name, _subscription):
        return None

    async def run():
        helper.on_reconnected(reconnect_callback)
        helper._reconnect_group = fake_reconnect_group
        subscription = _test_subscription(callback)
        helper._subscriptions["multi-series-group"] = subscription
        helper._connected.set()
        helper._pause_push_processing()

        await helper._enqueue_callback(callback, pd.DataFrame({"value": [1]}), _test_push_metadata())
        await helper._restore_groups_after_open([("multi-series-group", subscription)])
        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert attempts >= 1
    assert events == ["push"]


def test_shutdown_releases_paused_push_processing_before_waiting_for_callbacks():
    helper = MultiSeriesDPSHelper("username", "api-key", reconnect_callback=lambda: None, auto_connect=False)
    events = []

    async def callback(_frame, _metadata):
        events.append("push")

    async def run():
        helper._pause_push_processing()
        await helper._enqueue_callback(callback, pd.DataFrame({"value": [1]}), _test_push_metadata())
        await asyncio.wait_for(helper._shutdown_async(wait_for_callbacks=True), timeout=1)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert events == ["push"]


def test_connect_is_safe_to_call_when_connection_task_is_already_running():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)

    class RunningTask:
        @staticmethod
        def done():
            return False

    helper._connection_task = RunningTask()
    helper._client_initialised.set()

    try:
        assert helper.connect(timeout=0.1) is helper
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)


def test_send_with_response_raises_signalr_completion_errors():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)

    class Completion:
        error = "temporary backend issue"
        result = None

    class FakeHubConnection:
        async def send(self, _method, _arguments, on_invocation):
            await on_invocation(Completion())

    async def run():
        helper.hub_connection = FakeHubConnection()
        return await helper._send_with_response("ReconnectToPush", ["multi-series-group"], timeout=1)

    try:
        asyncio.run(run())
    except EnactApiError as exc:
        assert exc.error_code == "SignalRError"
        assert exc.message == "temporary backend issue"
    else:
        raise AssertionError("Expected EnactApiError")


def test_timed_out_send_removes_pending_invocation_handler():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)

    class FakeHubConnection:
        def __init__(self):
            self._invocation_handlers = {}

        async def send(self, _method, _arguments, on_invocation):
            self._invocation_handlers["invocation-id"] = on_invocation

    async def run():
        helper.hub_connection = FakeHubConnection()
        try:
            await helper._send_with_response("ReconnectToPush", ["multi-series-group"], timeout=0.01)
        except TimeoutError:
            return helper.hub_connection._invocation_handlers
        raise AssertionError("Expected TimeoutError")

    assert asyncio.run(run()) == {}


def test_cancelled_send_removes_pending_invocation_handler():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)

    class FakeHubConnection:
        def __init__(self):
            self._invocation_handlers = {}

        async def send(self, _method, _arguments, on_invocation):
            self._invocation_handlers["invocation-id"] = on_invocation

    async def run():
        helper.hub_connection = FakeHubConnection()
        task = asyncio.create_task(helper._send_with_response("ReconnectToPush", ["multi-series-group"], timeout=30))
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return helper.hub_connection._invocation_handlers
        raise AssertionError("Expected CancelledError")

    assert asyncio.run(run()) == {}


def test_transient_reconnect_failure_retries_reconnect_without_rejoining():
    helper = MultiSeriesDPSHelper(
        "username",
        "api-key",
        auto_connect=False,
        reconnect_initial_delay_seconds=0,
        reconnect_max_delay_seconds=0,
    )
    calls = []

    async def callback(_frame, _metadata):
        return None

    async def fake_send_with_response(method, arguments, *, timeout):
        calls.append((method, arguments, timeout))
        if len(calls) == 1:
            raise EnactApiError("ServiceUnavailable", "temporary backend issue", {})
        return {"data": {"pushName": "multi-series-group"}}

    async def run():
        subscription = helper._normalise_series_requests([{"seriesId": "RealtimeDemand", "countryId": "Gb"}])
        helper._subscriptions["multi-series-group"] = type(
            "Subscription",
            (),
            {"join_payload": subscription, "callback": callback, "parse_datetimes": True},
        )()
        helper._send_with_response = fake_send_with_response
        helper._connected.set()

        await helper._restore_group_until_connected("multi-series-group", helper._subscriptions["multi-series-group"])

    asyncio.run(run())

    assert calls == [
        ("ReconnectToPush", ["multi-series-group"], 30),
        ("ReconnectToPush", ["multi-series-group"], 30),
    ]


def test_zero_reconnect_delay_still_waits_between_restore_attempts(monkeypatch):
    helper = MultiSeriesDPSHelper(
        "username",
        "api-key",
        auto_connect=False,
        reconnect_initial_delay_seconds=0,
        reconnect_max_delay_seconds=0,
    )
    sleep_calls = []

    async def callback(_frame, _metadata):
        return None

    async def fake_reconnect_group(_group_name, _subscription):
        raise EnactApiError("ServiceUnavailable", "temporary backend issue", {})

    async def fake_sleep(delay):
        sleep_calls.append(delay)
        helper._connected.clear()

    async def run():
        subscription = helper._normalise_series_requests([{"seriesId": "RealtimeDemand", "countryId": "Gb"}])
        helper._subscriptions["multi-series-group"] = type(
            "Subscription",
            (),
            {"join_payload": subscription, "callback": callback, "parse_datetimes": True},
        )()
        helper._reconnect_group = fake_reconnect_group
        helper._connected.set()
        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        restored = await helper._restore_group_until_connected(
            "multi-series-group",
            helper._subscriptions["multi-series-group"],
        )
        assert restored is False

    asyncio.run(run())

    assert sleep_calls == [0.1]


def test_reconnect_completion_without_payload_is_success():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    calls = []

    async def callback(_frame, _metadata):
        return None

    async def fake_send_with_response(method, arguments, *, timeout):
        calls.append((method, arguments, timeout))
        return None

    async def run():
        subscription = helper._normalise_series_requests([{"seriesId": "RealtimeDemand", "countryId": "Gb"}])
        helper._subscriptions["multi-series-group"] = type(
            "Subscription",
            (),
            {"join_payload": subscription, "callback": callback, "parse_datetimes": True},
        )()
        helper._send_with_response = fake_send_with_response
        helper._connected.set()

        await helper._restore_group_until_connected("multi-series-group", helper._subscriptions["multi-series-group"])

    asyncio.run(run())

    assert calls == [("ReconnectToPush", ["multi-series-group"], 30)]


def test_reconnect_unavailable_response_rejoins_group():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    calls = []

    class FakeHubConnection:
        def on(self, _group_name, _callback):
            return None

    async def callback(_frame, _metadata):
        return None

    async def fake_send_with_response(method, arguments, *, timeout):
        calls.append((method, arguments, timeout))
        if method == "ReconnectToPush":
            raise EnactApiError("ReconnectUnavailable", "please call JoinMultiSeries again", {})
        return {"data": {"pushName": "new-multi-series-group"}}

    async def run():
        helper.hub_connection = FakeHubConnection()
        subscription = helper._normalise_series_requests([{"seriesId": "RealtimeDemand", "countryId": "Gb"}])
        helper._subscriptions["multi-series-group"] = type(
            "Subscription",
            (),
            {"join_payload": subscription, "callback": callback, "parse_datetimes": True},
        )()
        helper._subscription_group_by_request_key[helper._get_subscription_request_key(subscription)] = (
            "multi-series-group"
        )
        helper._send_with_response = fake_send_with_response
        helper._connected.set()

        await helper._restore_group_until_connected("multi-series-group", helper._subscriptions["multi-series-group"])

    asyncio.run(run())

    assert calls == [
        ("ReconnectToPush", ["multi-series-group"], 30),
        ("JoinMultiSeries", [[{"seriesId": "RealtimeDemand", "countryId": "Gb"}]], 30),
    ]
    assert helper.group_names == ("new-multi-series-group",)


def test_one_argument_callbacks_can_read_metadata_from_dataframe_attrs():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    seen_sequences = []

    def callback(frame):
        seen_sequences.append(frame.attrs["enact_push_metadata"].sequence)

    async def run():
        metadata = MultiSeriesPushMetadata(
            sequence=42,
            received_at_utc=datetime.now(timezone.utc),
            group_name="group",
            push_id="Gb&RealtimeDemand&none",
            series_id="RealtimeDemand",
            country_id="Gb",
        )
        frame = pd.DataFrame({"value": [1]})
        frame.attrs["enact_push_metadata"] = metadata

        await helper._enqueue_callback(callback, frame, metadata)
        await _wait_for_callback_processing(helper)

    try:
        asyncio.run(run())
    finally:
        if helper._callback_executor is not None:
            helper._callback_executor.shutdown(wait=True)

    assert seen_sequences == [42]


def test_falsey_callable_subscription_callback_is_still_used():
    helper = MultiSeriesDPSHelper("username", "api-key", callback=lambda _frame: None, auto_connect=False)

    class FalseyCallable:
        def __bool__(self):
            return False

        def __call__(self, _frame, _metadata):
            return None

    callback = FalseyCallable()

    assert helper._resolve_callback(callback) is callback


def test_identical_subscriptions_reuse_existing_group_without_joining_again():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    join_calls = []

    class FakeHubConnection:
        def on(self, _group_name, _callback):
            return None

    async def fake_send_with_response(method, arguments, *, timeout):
        join_calls.append((method, arguments, timeout))
        return {"data": {"pushName": "multi-series-group"}}

    async def callback(_frame, _metadata):
        return None

    async def replacement_callback(_frame, _metadata):
        return None

    async def run():
        helper.hub_connection = FakeHubConnection()
        helper._send_with_response = fake_send_with_response
        join_payload = helper._normalise_series_requests(
            [{"seriesId": "RealtimeDemand", "countryId": "Gb", "optionIds": [["none"]]}]
        )

        first_group_name = await helper._subscribe_join_payload(
            join_payload,
            callback,
            parse_datetimes=True,
            timeout=1,
        )
        second_group_name = await helper._subscribe_join_payload(
            join_payload,
            replacement_callback,
            parse_datetimes=True,
            timeout=1,
        )

        return first_group_name, second_group_name

    first_group_name, second_group_name = asyncio.run(run())

    assert first_group_name == "multi-series-group"
    assert second_group_name == "multi-series-group"
    assert len(join_calls) == 1
    assert helper._subscriptions["multi-series-group"].callback is replacement_callback


def test_equivalent_subscription_requests_reuse_group_when_order_changes():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    join_calls = []

    class FakeHubConnection:
        def on(self, _group_name, _callback):
            return None

    async def fake_send_with_response(method, arguments, *, timeout):
        join_calls.append((method, arguments, timeout))
        return {"data": {"pushName": "multi-series-group"}}

    async def callback(_frame, _metadata):
        return None

    async def run():
        helper.hub_connection = FakeHubConnection()
        helper._send_with_response = fake_send_with_response
        first_payload = helper._normalise_series_requests(
            [
                {"seriesId": "RealtimeDemand", "countryId": "Gb"},
                {"seriesId": "RealtimeFrequency", "countryId": "Gb", "optionIds": [["B"], ["A"]]},
            ]
        )
        reordered_payload = helper._normalise_series_requests(
            [
                {"seriesId": "RealtimeFrequency", "countryId": "Gb", "optionIds": [["A"], ["B"]]},
                {"seriesId": "RealtimeDemand", "countryId": "Gb"},
            ]
        )

        first_group_name = await helper._subscribe_join_payload(
            first_payload,
            callback,
            parse_datetimes=True,
            timeout=1,
        )
        second_group_name = await helper._subscribe_join_payload(
            reordered_payload,
            callback,
            parse_datetimes=True,
            timeout=1,
        )

        return first_group_name, second_group_name

    first_group_name, second_group_name = asyncio.run(run())

    assert first_group_name == "multi-series-group"
    assert second_group_name == "multi-series-group"
    assert len(join_calls) == 1


def test_unsubscribe_unknown_group_is_safe_noop():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)

    assert helper.unsubscribe_from_chart_pushes("not-subscribed") is False
    assert helper.group_names == ()


def test_unsubscribe_removes_subscription_and_leaves_group_when_connected():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    calls = []

    class FakeHubConnection:
        def on(self, _group_name, _callback):
            return None

    async def fake_send_with_response(method, arguments, *, timeout):
        calls.append((method, arguments, timeout))
        if method == "JoinMultiSeries":
            return {"data": {"pushName": "multi-series-group"}}
        return {}

    async def callback(_frame, _metadata):
        return None

    async def run():
        helper.hub_connection = FakeHubConnection()
        helper._send_with_response = fake_send_with_response

        join_payload = helper._normalise_series_requests([{"seriesId": "RealtimeDemand", "countryId": "Gb"}])
        await helper._subscribe_join_payload(join_payload, callback, parse_datetimes=True, timeout=1)
        helper._connected.set()

        return await helper._unsubscribe_group("multi-series-group", timeout=1)

    unsubscribed = asyncio.run(run())

    assert unsubscribed is True
    assert helper.group_names == ()
    assert helper._subscription_group_by_request_key == {}
    assert calls == [
        ("JoinMultiSeries", [[{"seriesId": "RealtimeDemand", "countryId": "Gb"}]], 1),
        ("LeaveGroup", ["multi-series-group"], 1),
    ]


def test_shutdown_closes_underlying_websocket_before_cancelling_connection_task():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    events = []

    class FakeWebSocket:
        async def close(self):
            events.append("close")

    class FakeTransport:
        _ws = FakeWebSocket()

    class FakeHubConnection:
        _transport = FakeTransport()

    async def run_until_cancelled():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("cancel")
            raise

    async def run():
        helper.hub_connection = FakeHubConnection()
        helper._connection_task = asyncio.create_task(run_until_cancelled())
        await asyncio.sleep(0)
        await helper._shutdown_async(wait_for_callbacks=False)

    asyncio.run(run())

    assert events == ["close", "cancel"]


def test_shutdown_leaves_known_groups_before_closing_websocket():
    helper = MultiSeriesDPSHelper("username", "api-key", auto_connect=False)
    events: list[Any] = []

    class FakeWebSocket:
        async def close(self):
            events.append("close")

    class FakeTransport:
        _ws = FakeWebSocket()

    class FakeHubConnection:
        _transport = FakeTransport()

    async def fake_send_with_response(method, arguments, *, timeout):
        events.append((method, arguments, timeout))
        return {}

    async def run_until_cancelled():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("cancel")
            raise

    async def run():
        helper.hub_connection = FakeHubConnection()
        helper._send_with_response = fake_send_with_response
        helper._subscriptions["multi-series-group"] = object()
        helper._subscription_group_by_request_key["request-key"] = "multi-series-group"
        helper._connected.set()
        helper._connection_task = asyncio.create_task(run_until_cancelled())

        await asyncio.sleep(0)
        await helper._shutdown_async(wait_for_callbacks=False, leave_groups_timeout=1)

    asyncio.run(run())

    assert events == [("LeaveGroup", ["multi-series-group"], 1), "close", "cancel"]
    assert helper.group_names == ()
    assert helper._subscription_group_by_request_key == {}
