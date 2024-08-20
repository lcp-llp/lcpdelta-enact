import pytest
import httpx
import tenacity

from datetime import date
from tests.integration import enact_api_helper
from lcp_delta.common.http.exceptions import EnactApiError

default_policy_retry_no = 3

mock_url = "http://example.com/api"
mock_request_body = {"testPostKey": "testPostValue"}
mock_success_response_body = {"mockResponseKey": "mockResponseValue"}

mock_request = httpx.Request("POST", mock_url, json=mock_request_body)

mock_response_401 = httpx.Response(status_code=401, headers={"WWW-Authenticate": "Bearer"}, request=mock_request)


def create_mock_response(status_code: int):
    return httpx.Response(status_code=status_code, request=mock_request)


def test_transient_401_triggers_token_refresh(mocker):
    mock_post = mocker.patch("httpx.Client.post")
    mock_post.side_effect = [
        httpx.Response(status_code=401, headers={"WWW-Authenticate": "Bearer"}),
        httpx.Response(status_code=200, json=mock_success_response_body),
    ]

    mock_retry = mocker.patch.object(enact_api_helper, "_retry_with_refreshed_token")
    mock_retry.return_value = httpx.Response(status_code=200, json=mock_success_response_body)

    response = enact_api_helper._post_request(mock_url, mock_request_body)

    mock_retry.assert_called_once()
    assert response == mock_success_response_body


def test_persistent_401_does_not_trigger_infinite_retries(mocker):
    mock_post = mocker.patch("httpx.Client.post")
    mock_post.side_effect = [mock_response_401] * 100

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        enact_api_helper._post_request(mock_url, mock_request_body)

    assert "401 Unauthorised", mock_url in str(exc_info.value)
    assert mock_post.call_count == 3


@pytest.mark.parametrize("status_code", [500, 503, 504, 408, 429])
def test_retries_when_intended(mocker, status_code):
    mock_post = mocker.patch("httpx.Client.post")
    mock_post.return_value = create_mock_response(status_code)

    with pytest.raises(tenacity.RetryError) as exc_info:
        enact_api_helper._post_request(mock_url, mock_request_body)

    assert str(status_code), mock_url in str(exc_info.value)
    assert mock_post.call_count == default_policy_retry_no


@pytest.mark.parametrize("status_code", [403, 404])
def test_does_not_retry_when_not_intended(mocker, status_code):
    mock_post = mocker.patch("httpx.Client.post")
    mock_post.return_value = create_mock_response(status_code)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        enact_api_helper._post_request(mock_url, mock_request_body)

    assert str(status_code), mock_url in str(exc_info.value)
    assert mock_post.call_count == 1


@pytest.mark.parametrize(
    "status_code, error_code, error_message",
    [
        (400, "MissingData", "One of the required properties on the Request JSON is missing"),
        (400, "UnknownData", "One of the properties sent does not match from the option list"),
    ],
)
def test_get_series_data_bubbles_up_4xx_errors(mocker, status_code, error_code, error_message):
    mock_post = mocker.patch("httpx.Client.post")
    mock_post.return_value = httpx.Response(
        status_code=status_code,
        json={"messages": [{"errorCode": error_code, "message": error_message}]},
        request=mock_request,
    )

    with pytest.raises(EnactApiError) as exc_info:
        enact_api_helper.get_series_data(
            "DayAheadPrices", date(2024, 8, 1), date(2024, 8, 1), country_id="Gb", time_zone_id="UTC"
        )

    assert exc_info.value.error_code == error_code
    assert exc_info.value.message == error_message
    assert exc_info.value.response.status_code == status_code
