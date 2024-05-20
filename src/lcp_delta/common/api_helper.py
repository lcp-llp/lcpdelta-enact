import requests
import json
from abc import ABC

from ..enact.credentials_holder import CredentialsHolder


class APIHelperBase(ABC):
    def __init__(self, username: str, public_api_key: str):
        """Enter your credentials and use the methods below to get data from Enact.

        Args:
            username `str`: Enact Username. Please contact the Enact team if you are unsure about what your username or public api key are.
            public_api_key `str`: Public API Key provided by Enact. Please contact the Enact team if you are unsure about what your username or public api key are.
        """
        self.enact_credentials = CredentialsHolder(username, public_api_key)

    # TODO: chat about if these should be async by default instead of sync
    def post_request(self, endpoint: str, request_details: dict, verify=None):
        headers = {
            "Authorization": "Bearer " + self.enact_credentials.bearer_token,
            "Content-Type": "application/json",
            "cache-control": "no-cache",
        }

        # TODO: chat about if maybe this should use httpx instead of requests given async support and https://www.reddit.com/r/Python/comments/17swe3a/requests_3_news/
        response_raw = requests.post(endpoint, data=json.dumps(request_details), headers=headers, verify=verify)

        # TODO: probably want to do this via response.raise_for_status()
        if response_raw.status_code != 200:
            response_raw = self.handle_error_and_get_updated_response(endpoint, request_details, headers, response_raw)

        # TODO: this would only be a string if you overwrite the var above, and I wouldnt do that. All err processing should happening in that helper
        if isinstance(response_raw, str):
            raise Exception(f"{response_raw}")
        if "messages" in response_raw:
            self.raise_exception_for_enact_error(response_raw)

        # TODO: can access via response.json()
        response = json.loads(response_raw.text)
        return response

    # TODO: missing type hints
    def handle_error_and_get_updated_response(self, endpoint: str, request_details: dict, headers, response_raw):
        # check if bearer token has expired and if it has create a new one
        if response_raw.status_code == 401 and "WWW-Authenticate" in response_raw.headers:
            response_raw = self.handle_authorisation_error(endpoint, request_details, headers)

        if response_raw.status_code == 400:
            self.raise_exception_for_enact_error(json.loads(response_raw.text))
        response = json.loads(response_raw.text)
        return response

    def raise_exception_for_enact_error(self, response):
        error_messages = response["messages"]
        for error_message in error_messages:
            if "errorCode" in error_message and error_message["errorCode"]:
                # An error code is present, so raise an exception with the error message
                raise Exception(f'ErrorCode: {error_message["errorCode"]}. {error_message["message"]}')

    def handle_authorisation_error(self, endpoint: str, request_details: dict, headers: dict):
        retry_count = 0
        while retry_count < self.enact_credentials.MAX_RETRIES:
            # TODO: I dont understand the below - get_bearer_token doesnt take arguments. Does this func work?
            self.enact_credentials.get_bearer_token(
                self.enact_credentials.username, self.enact_credentials.public_api_key
            )
            headers["Authorization"] = "Bearer " + self.enact_credentials.bearer_token

            # Retry the POST request with the new bearer token
            # TODO: dont need to do data=json.dumps can just json=request_details
            response = requests.post(endpoint, data=json.dumps(request_details), headers=headers)

            if response.status_code != 401:
                # Successful response, no need to retry
                break

            retry_count += 1

        if retry_count == self.enact_credentials.MAX_RETRIES:
            # TODO: Id raise something other than a generic exception here
            raise Exception("Failed to obtain a valid bearer token after multiple attempts.")
        return response
