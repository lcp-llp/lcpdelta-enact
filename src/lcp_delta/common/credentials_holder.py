import json
import httpx
import threading

from datetime import datetime

from lcp_delta.common import constants
from lcp_delta.common.response_objects.usage_info import UsageInfo
from lcp_delta.common.http.retry_policies import DEFAULT_RETRY_POLICY


class CredentialsHolder:
    def __init__(self, username: str, public_api_key: str):
        self._token_lock = threading.Lock()
        self._auth_headers = {"Content-Type": "application/json", "cache-control": "no-cache"}
        self._credentials_payload = {"Username": username, "ApiKey": public_api_key}
        self.get_bearer_token()

    @property
    def bearer_token(self):
        with self._token_lock:
            return self._bearer_token

    @bearer_token.setter
    def bearer_token(self, value):
        with self._token_lock:
            self._bearer_token = value

    @DEFAULT_RETRY_POLICY
    def get_bearer_token(self):
        """
        Gets the bearer token for authentication, based on the username and public API key associated with the instance.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/auth/token"
        with httpx.Client(verify=True) as client:
            response = client.post(endpoint, headers=self._auth_headers, json=self._credentials_payload)

        self.bearer_token = response.text

    @DEFAULT_RETRY_POLICY
    def get_remaining_token_count(self) -> UsageInfo:
        """
        Gets the monthly quota and remaining call count for a particular account, based on the username and public API key associated with the instance.

        Returns:
            `UsageInfo`: An object holding the monthly quota, remaining allowance, and date that monthly usage was last last refreshed.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/auth/usage_v2"
        with httpx.Client(verify=True) as client:
            response = client.post(endpoint, headers=self._auth_headers, json=self._credentials_payload)

        response_data = json.loads(response.content)

        return UsageInfo(
            response_data["remainingCallsForMonth"],
            response_data["monthlyCallAllowance"],
            datetime.strptime(response_data["dateLastRenewed"], "%Y-%m-%dT%H:%M:%S"),
            response_data["unlimitedUsage"],
        )
