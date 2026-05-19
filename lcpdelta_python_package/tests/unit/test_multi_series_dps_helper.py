import asyncio
from datetime import datetime, timezone

import pandas as pd

from lcp_delta.enact.multi_series_dps_helper import MultiSeriesDPSHelper, MultiSeriesPushMetadata


async def _wait_for_callback_processing(helper):
    for queue in helper._callback_queues:
        await queue.join()
    for task in helper._callback_tasks:
        task.cancel()
    await asyncio.gather(*helper._callback_tasks, return_exceptions=True)


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
    started_by_push_id = {first_push_id: [], second_push_id: []}
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
    events = []

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
