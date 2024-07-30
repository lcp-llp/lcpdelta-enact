from datetime import datetime
import httpx
import json
import threading
from ..enact.response_objects.usage_info import UsageInfo
from . import constants


class CredentialsHolder:
    MAX_RETRIES = 3

    def __init__(self, username: str, public_api_key: str):
        self.username = username
        self.public_api_key = public_api_key
        self._token_lock = threading.Lock()
        self.get_bearer_token()

    @property
    def bearer_token(self):
        with self._token_lock:
            return self._bearer_token

    @bearer_token.setter
    def bearer_token(self, value):
        with self._token_lock:
            self._bearer_token = value

    @constants.DEFAULT_HTTP_RETRY_POLICY
    def get_bearer_token(self):
        """Get the bearer token for authentication.

        This method sends a request to obtain a bearer token for authentication
        using the Enact API. It takes an Enact username and public API key as
        input parameters.

        Args:
            username `str`: The username for Enact authentication.
            public_api_key `str`: The public API key for Enact authentication.
        """

        headers = {"Content-Type": "application/json", "cache-control": "no-cache"}
        data = {"Username": self.username, "ApiKey": self.public_api_key}
        with httpx.Client(verify=True) as client:
            response = client.post("https://enactapifd.lcp.uk.com/auth/token", headers=headers, json=data)

        if response.status_code == 401 or (response.status_code >= 500 and response.status_code < 600):
            response = self.retry_request(headers, data)

        self.bearer_token = response.text

    @constants.DEFAULT_HTTP_RETRY_POLICY
    def retry_request(self, headers, data):
        """Retry the request to obtain a valid bearer token.

        This method retries the request to obtain a valid bearer token for authentication. It takes the request headers and data as input parameters.

        Args:
            headers `dict`: The headers for the request.
            data `dict`: The data to be sent with the request.

        Raises:
            Exception: If a valid bearer token cannot be obtained after
                    multiple attempts.

        Returns:
            Response: The response object containing the bearer token.
        """
        retry_count = 0
        while retry_count < self.MAX_RETRIES:
            with httpx.Client(verify=True) as client:
                response = client.post("https://enactapifd.lcp.uk.com/auth/token", headers=headers, json=data)
            if response.status_code != 401 and (response.status_code < 500 or response.status_code >= 600):
                # Successful response, no need to retry
                break
            retry_count += 1

        if retry_count == self.MAX_RETRIES:
            raise Exception("Failed to obtain a valid bearer token after multiple attempts.")

        return response

    @constants.DEFAULT_HTTP_RETRY_POLICY
    def get_remaining_token_count(self) -> UsageInfo:
        """Get the remaining token count for API calls.

        This method sends a request to obtain the remaining token count for API calls. It retrieves the count based on the Enact username and public API key associated with the instance.

        Returns:
            `str`: The remaining token count for API calls.
        """
        headers = {"Content-Type": "application/json", "cache-control": "no-cache"}
        data = {"Username": self.username, "ApiKey": self.public_api_key}

        endpoint = "https://enactapifd.lcp.uk.com/auth/usage_v2"

        with httpx.Client(verify=True) as client:
            response = client.post(endpoint, headers=headers, json=data)

        if response.status_code != 200:
            if response.status_code == 401 or (response.status_code >= 500 and response.status_code < 600):
                response = self.retry_request(headers, data)
            if response.status_code == 404:
                raise httpx.HTTPStatusError(f"Error: {response.text}", request=response.request, response=response)

        data = json.loads(response.content)

        return UsageInfo(
            data["remainingCallsForMonth"],
            data["monthlyCallAllowance"],
            datetime.strptime(data["dateLastRenewed"], "%Y-%m-%dT%H:%M:%S"),
            data["unlimitedUsage"],
        )
