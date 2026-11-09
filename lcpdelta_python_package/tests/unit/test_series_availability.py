import asyncio
import json
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from lcp_delta import enact
from lcp_delta.common.http.exceptions import EnactApiError


@pytest.fixture
def helper(monkeypatch):
    def fake_get_bearer_token(credentials):
        credentials.bearer_token = "test-token"

    monkeypatch.setattr(enact.CredentialsHolder, "get_bearer_token", fake_get_bearer_token)
    return enact.APIHelper("username", "api-key")


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("first_date", ["2020-01-01", None])
@pytest.mark.parametrize(
    "series_id, options",
    [
        ("SkipRateAllBm", ["DRAXX-4", "Offer", "Five", "HalfHourly"]),
        ("OutturnFuel", ["Coal"]),
        ("Pn", ["DRAXX-4"]),
        ("RealtimeDemand", None),
        ("RealtimeDemand", []),
    ],
)
def test_availability_posts_exact_selection_and_returns_json(
    helper, monkeypatch, asynchronous, first_date, series_id, options
):
    requests = []
    response_body = {"firstAvailableDate": first_date}

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json=response_body)

    transport = httpx.MockTransport(handle)
    client_type = httpx.AsyncClient if asynchronous else httpx.Client
    client_factory = lambda **kwargs: client_type(transport=transport, **kwargs)
    monkeypatch.setattr(httpx, "AsyncClient" if asynchronous else "Client", client_factory)

    if asynchronous:
        result = asyncio.run(helper.get_series_availability_async(series_id, "Gb", option_id=options))
    else:
        result = helper.get_series_availability(series_id, "Gb", option_id=options)

    assert result == response_body
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == f"{helper.endpoints._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/availability"
    assert request.headers["Authorization"] == "Bearer test-token"
    assert json.loads(request.content) == {"SeriesId": series_id, "CountryId": "Gb", "OptionId": options}


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("options", ["Coal", ("Coal",), ["DRAXX-4", 5], [["Coal"]]])
def test_invalid_options_fail_before_request(helper, monkeypatch, asynchronous, options):
    post = AsyncMock() if asynchronous else Mock()
    monkeypatch.setattr(helper, "_post_request_async" if asynchronous else "_post_request", post)

    with pytest.raises(ValueError, match="Option ID input must be a list of strings"):
        if asynchronous:
            asyncio.run(helper.get_series_availability_async("OutturnFuel", "Gb", option_id=options))
        else:
            helper.get_series_availability("OutturnFuel", "Gb", option_id=options)

    post.assert_not_called()


@pytest.mark.parametrize("asynchronous", [False, True])
def test_availability_preserves_api_validation_errors(helper, monkeypatch, asynchronous):
    error_body = {"errors": {"OptionId": ["Invalid option selection."]}, "status": 400}
    transport = httpx.MockTransport(lambda request: httpx.Response(400, json=error_body))
    client_type = httpx.AsyncClient if asynchronous else httpx.Client
    client_factory = lambda **kwargs: client_type(transport=transport, **kwargs)
    monkeypatch.setattr(httpx, "AsyncClient" if asynchronous else "Client", client_factory)

    with pytest.raises(EnactApiError) as error:
        if asynchronous:
            asyncio.run(helper.get_series_availability_async("SkipRateAllBm", "Gb"))
        else:
            helper.get_series_availability("SkipRateAllBm", "Gb")

    assert error.value.response.status_code == 400
    assert error.value.response.json() == error_body
