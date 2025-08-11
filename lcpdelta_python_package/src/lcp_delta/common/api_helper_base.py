import httpx
from abc import ABC
from json import JSONDecodeError
from typing import Callable

from lcp_delta.common.credentials_holder import CredentialsHolder
from lcp_delta.common.api_endpoints import EnactApiEndpoints
from lcp_delta.common.http.retry_policies import DEFAULT_RETRY_POLICY, UNAUTHORISED_INCLUSIVE_RETRY_POLICY
from lcp_delta.common.http.exceptions import EnactApiError

class APIHelperBase(ABC):
    def __init__(self, username: str, public_api_key: str, bypass_frontdoor: bool = False):
        """Enter your credentials and use the methods below to get data from Enact.

        Args:
            username `str`: Enact Username. Please contact the Enact team if you are unsure about what your username or public api key are.
            public_api_key `str`: Public API Key provided by Enact. Please contact the Enact team if you are unsure about what your username or public api key are.
        """
        self.credentials_holder = CredentialsHolder(username, public_api_key)
        self.enact_credentials = self.credentials_holder  # legacy
        self.timeout = httpx.Timeout(5.0, read=60.0)
        self._bypass_frontdoor = bypass_frontdoor
        self.endpoints = EnactApiEndpoints(self.credentials_holder, self._bypass_frontdoor)

    @DEFAULT_RETRY_POLICY
    async def _post_request_async(self, endpoint: str, request_body: dict, long_timeout: bool = False):
        timeout = httpx.Timeout(5.0, read=60.0) if long_timeout else self.timeout

        async def make_request(ep):
            async with httpx.AsyncClient(verify=True, timeout=timeout) as client:
                return await client.post(ep, json=request_body, headers=self._get_headers())

        response = await make_request(endpoint)

        if response.status_code == 503 and self._bypass_frontdoor:
            response = self._refresh_fd_bypass_and_retry_on_503(endpoint, make_request)

        if response.status_code == 401 and "WWW-Authenticate" in response.headers:
            response = await self._retry_with_refreshed_token_async(endpoint, headers=self._get_headers(), request_body=request_body)

        if response.status_code != 200:
            self._handle_unsuccessful_response(response)

        return response.json()

    @DEFAULT_RETRY_POLICY
    def _post_request(self, endpoint: str, request_body: dict, long_timeout: bool = False):
        timeout = httpx.Timeout(5.0, read=60.0) if long_timeout else self.timeout

        def make_request(ep):
            with httpx.Client(verify=True, timeout=timeout) as client:
                return client.post(ep, json=request_body, headers=self._get_headers())

        response = make_request(endpoint)

        if response.status_code == 503 and self._bypass_frontdoor:
            response = self._refresh_fd_bypass_and_retry_on_503(endpoint, make_request)

        if response.status_code == 401 and "WWW-Authenticate" in response.headers:
            response = self._retry_with_refreshed_token(endpoint, headers=self._get_headers(), request_body=request_body)

        if response.status_code != 200:
            self._handle_unsuccessful_response(response)

        return response.json()

    @DEFAULT_RETRY_POLICY
    async def _get_request_async(self, endpoint: str, params: dict = None, long_timeout: bool = False):
        timeout = httpx.Timeout(5.0, read=60.0) if long_timeout else self.timeout

        async def make_request(ep):
            async with httpx.AsyncClient(verify=True, timeout=timeout) as client:
                return await client.get(ep, params=params, headers=self._get_headers())

        response = await make_request(endpoint)

        if response.status_code == 503 and self._bypass_frontdoor:
            response = self._refresh_fd_bypass_and_retry_on_503(endpoint, make_request)

        if response.status_code == 401 and "WWW-Authenticate" in response.headers:
            response = await self._retry_with_refreshed_token_async(endpoint, params=params, headers=self._get_headers(), method="GET")

        if response.status_code != 200:
            self._handle_unsuccessful_response(response)

        return response.json()

    @DEFAULT_RETRY_POLICY
    def _get_request(self, endpoint: str, params: dict = None, long_timeout: bool = False):
        timeout = httpx.Timeout(5.0, read=60.0) if long_timeout else self.timeout

        def make_request(ep):
            with httpx.Client(verify=True, timeout=timeout) as client:
                return client.get(ep, params=params, headers=self._get_headers())

        response = make_request(endpoint)

        if response.status_code == 503 and self._bypass_frontdoor:
            response = self._refresh_fd_bypass_and_retry_on_503(endpoint, make_request)

        if response.status_code == 401 and "WWW-Authenticate" in response.headers:
            response = self._retry_with_refreshed_token(endpoint, headers=self._get_headers(), params=params, method="GET")

        if response.status_code != 200:
            self._handle_unsuccessful_response(response)

        return response.json()

    @UNAUTHORISED_INCLUSIVE_RETRY_POLICY
    async def _retry_with_refreshed_token_async(self, endpoint: str, headers: dict, request_body: dict = None, params: dict = None, method: str = "POST"):
        """
        Retries a given request with a refreshed bearer token asynchronously.
        """
        self._refresh_headers(headers)

        async with httpx.AsyncClient(verify=False, timeout=self.timeout) as client:
            if method.upper() == "POST":
                return await client.post(endpoint, json=request_body, headers=headers)
            elif method.upper() == "GET":
                return await client.get(endpoint, params=params, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

    @UNAUTHORISED_INCLUSIVE_RETRY_POLICY
    def _retry_with_refreshed_token(self, endpoint: str, headers: dict, request_body: dict = None, params: dict = None, method: str = "POST"):
        """
        Retries a given request with a refreshed bearer token.
        """
        self._refresh_headers(headers)

        with httpx.Client(verify=False, timeout=self.timeout) as client:
            if method.upper() == "POST":
                return client.post(endpoint, json=request_body, headers=headers)
            elif method.upper() == "GET":
                return client.get(endpoint, params=params, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

    def _get_headers(self):
        return {
            "Authorization": "Bearer " + self.credentials_holder.bearer_token,
            "Content-Type": "application/json",
            "cache-control": "no-cache",
        }

    def _refresh_headers(self, headers: dict):
        self.credentials_holder.get_bearer_token()
        headers["Authorization"] = "Bearer " + self.credentials_holder.bearer_token

    def _handle_unsuccessful_response(self, response: httpx.Response):
        try:
            response_data = response.json()
            if response.text != "" and "messages" in response_data:
                error_messages = response_data["messages"]
                for error_message in error_messages:
                    if "errorCode" in error_message and error_message["errorCode"]:
                        raise EnactApiError(error_message["errorCode"], error_message["message"], response)
        except (ValueError, JSONDecodeError):
            pass

        response.raise_for_status()

    def _refresh_fd_bypass_and_retry_on_503(self, endpoint: str, retry_func: Callable):
        self.endpoints.refresh_fd_bypass()
        new_endpoint = self.endpoints.rebuild_endpoint(endpoint)
        return retry_func(new_endpoint)
