from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pandas as pd
import pytest

from lcp_delta.enact import APIHelper
from lcp_delta.enact.services import niv_evolution_service as service

DAY = datetime(2026, 11, 10, tzinfo=timezone.utc)
OPTIONS = ["NivOutturn", "AcceptedOfferVolume"]
ROW = ["10-11-2026", 1, "NivOutturn", "2026-11-09T22:30:00Z", -12.5]
PAYLOAD = {"data": [service.COLUMNS, ROW]}


def helper():
    api = object.__new__(APIHelper)
    api.endpoints = SimpleNamespace(
        NIV_EVOLUTION="periodData", NIV_EVOLUTION_DAILY="dailyData", NIV_EVOLUTION_DATE_RANGE="rangedData"
    )
    return api


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("kind", ["period", "day", "date_range"])
@pytest.mark.asyncio
async def test_public_methods_and_pagination(kind, asynchronous):
    api = helper()
    pages = [{"data": PAYLOAD, "nextCursor": "2026-11-11"}, {"data": PAYLOAD, "nextCursor": None}]
    request = AsyncMock(side_effect=pages) if asynchronous else Mock(side_effect=pages)
    if kind != "date_range":
        request.side_effect = None
        request.return_value = PAYLOAD
    setattr(api, "_get_request_async" if asynchronous else "_get_request", request)
    method = getattr(api, f"get_niv_evolution_for_{kind}" + ("_async" if asynchronous else ""))
    args = {
        "period": (1, DAY, OPTIONS),
        "day": (DAY, OPTIONS),
        "date_range": (DAY, datetime(2026, 11, 11), OPTIONS),
    }[kind]
    result = await method(*args) if asynchronous else method(*args)
    expected = pd.DataFrame([ROW] * (2 if kind == "date_range" else 1), columns=service.COLUMNS)
    pd.testing.assert_frame_equal(result, expected)
    first = {"options": OPTIONS}
    first.update({"start": "2026-11-10", "end": "2026-11-11"} if kind == "date_range" else {"date": "2026-11-10"})
    if kind == "period":
        first["period"] = 1
    endpoint = {"period": "periodData", "day": "dailyData", "date_range": "rangedData"}[kind]
    assert request.call_args_list[0].args == (endpoint, first)
    assert request.call_args_list[0].kwargs == {"long_timeout": True}
    if kind == "date_range":
        assert request.call_args_list[1].args[1] == {"cursor": "2026-11-11", "end": "2026-11-11", "options": OPTIONS}
    if asynchronous:
        assert request.await_count == request.call_count


def test_options_are_repeated_query_parameters():
    params = service.generate_by_period_request(1, DAY, OPTIONS)
    query = httpx.Request("GET", "https://example.test/periodData", params=params).url.params
    assert query.get_list("options") == OPTIONS
    assert query["date"] == "2026-11-10"
    assert query["period"] == "1"


@pytest.mark.parametrize("data", [[], [service.COLUMNS]])
def test_empty_response_preserves_columns(data):
    result = service.process_response({"data": data})
    assert result.empty
    assert list(result.columns) == service.COLUMNS


@pytest.mark.parametrize("response", [None, {"data": None}])
def test_null_is_reported_as_failure(response):
    with pytest.raises(ValueError, match="NIV evolution API returned null"):
        service.process_response(response)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_range_empty_page_still_follows_cursor(asynchronous, capsys):
    api = helper()
    pages = [{"data": {"data": []}, "nextCursor": "2026-11-11"}, {"data": PAYLOAD, "nextCursor": None}]
    request = AsyncMock(side_effect=pages) if asynchronous else Mock(side_effect=pages)
    setattr(api, "_get_request_async" if asynchronous else "_get_request", request)
    args = (DAY, datetime(2026, 11, 11), OPTIONS)
    result = (
        await api.get_niv_evolution_for_date_range_async(*args)
        if asynchronous
        else api.get_niv_evolution_for_date_range(*args)
    )
    assert result.values.tolist() == [ROW]
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("response", [None, {"data": {"data": []}, "nextCursor": None}])
@pytest.mark.asyncio
async def test_range_null_or_entirely_empty(asynchronous, response):
    api = helper()
    request = AsyncMock(return_value=response) if asynchronous else Mock(return_value=response)
    setattr(api, "_get_request_async" if asynchronous else "_get_request", request)
    method = getattr(api, "get_niv_evolution_for_date_range" + ("_async" if asynchronous else ""))
    if response is None:
        with pytest.raises(ValueError, match="NIV evolution API returned null"):
            if asynchronous:
                await method(DAY, DAY, OPTIONS)
            else:
                method(DAY, DAY, OPTIONS)
    else:
        result = await method(DAY, DAY, OPTIONS) if asynchronous else method(DAY, DAY, OPTIONS)
        assert result.empty
        assert list(result.columns) == service.COLUMNS