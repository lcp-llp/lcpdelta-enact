import httpx

from abc import ABC

from lcp_delta.common.credentials_holder import CredentialsHolder
from lcp_delta.common.http.retry_policies import DEFAULT_RETRY_POLICY, UNAUTHORISED_INCLUSIVE_RETRY_POLICY
from lcp_delta.common.http.exceptions import EnactApiError


class APIHelperBase(ABC):
    def __init__(self, username: str, public_api_key: str):
        """Enter your credentials and use the methods below to get data from Enact.

        Args:
            username `str`: Enact Username. Please contact the Enact team if you are unsure about what your username or public api key are.
            public_api_key `str`: Public API Key provided by Enact. Please contact the Enact team if you are unsure about what your username or public api key are.
        """
        self.credentials_holder = CredentialsHolder(username, public_api_key)
        self.enact_credentials = self.credentials_holder  # legacy

    @DEFAULT_RETRY_POLICY
    async def _post_request_async(self, endpoint: str, request_body: dict):
        """
        Sends a post request with a given payload to a given endpoint asynchronously.
        """
        async with httpx.AsyncClient(verify=True) as client:
            response = await client.post(endpoint, json=request_body, headers=self._get_headers())

        # if bearer token expired, refresh and retry
        if response.status_code == 401 and "WWW-Authenticate" in response.headers:
            response = await self._retry_with_refreshed_token_async(endpoint, request_body, self._get_headers())

        if response.status_code != 200:
            self._handle_unsuccessful_response(response)

        return response.json()

    @DEFAULT_RETRY_POLICY
    def _post_request(self, endpoint: str, request_body: dict):
        """
        Sends a post request with a given payload to a given endpoint.
        """
        with httpx.Client(verify=True) as client:
            response = client.post(endpoint, json=request_body, headers=self._get_headers())

        # if bearer token expired, refresh and retry
        if response.status_code == 401 and "WWW-Authenticate" in response.headers:
            response = self._retry_with_refreshed_token(endpoint, request_body, self._get_headers())

        if response.status_code != 200:
            self._handle_unsuccessful_response(response)

        return response.json()

    @UNAUTHORISED_INCLUSIVE_RETRY_POLICY
    async def _retry_with_refreshed_token_async(self, endpoint: str, request_body: dict, headers: dict):
        """
        Retries a given POST request with a refreshed bearer token asynchronously.
        """
        self._refresh_headers(headers)

        async with httpx.AsyncClient(verify=True) as client:
            return await client.post(endpoint, json=request_body, headers=headers)

    @UNAUTHORISED_INCLUSIVE_RETRY_POLICY
    def _retry_with_refreshed_token(self, endpoint: str, request_body: dict, headers: dict):
        """
        Retries a given POST request with a refreshed bearer token.
        """
        self._refresh_headers(headers)

        with httpx.Client(verify=True) as client:
            return client.post(endpoint, json=request_body, headers=headers)

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
        response_data = response.json()
        if response.text != "" and "messages" in response_data:
            error_messages = response_data["messages"]
            for error_message in error_messages:
                if "errorCode" in error_message and error_message["errorCode"]:
                    raise EnactApiError(error_message["errorCode"], error_message["message"], response)
        else:
            response.raise_for_status()
