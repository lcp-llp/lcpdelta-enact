import httpx
from abc import ABC
from functools import wraps
import asyncio
from .constants import DEFAULT_HTTP_RETRY_POLICY
import nest_asyncio

nest_asyncio.apply()


def async_to_sync(func):
    @wraps(func)
    def sync_func(*args, **kwargs):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(func(*args, **kwargs))
        else:
            task = loop.create_task(func(*args, **kwargs))
            return loop.run_until_complete(task)

    return sync_func


def add_sync_methods(cls):
    for attr_name in dir(cls):
        if attr_name.startswith("_"):
            continue
        attr = getattr(cls, attr_name)
        if asyncio.iscoroutinefunction(attr):
            if attr_name.endswith("_async"):
                new_attr_name = attr_name[:-6]
                setattr(cls, new_attr_name, async_to_sync(attr))
            else:
                raise ValueError(f"Public async function '{attr_name}' must have an '_async' suffix")
    return cls


class APIHelperBase(ABC):
    def __init__(self, username: str, public_api_key: str):
        """Enter your credentials and use the methods below to get data from Enact.

        Args:
            username `str`: Enact Username. Please contact the Enact team if you are unsure about what your username or public api key are.
            public_api_key `str`: Public API Key provided by Enact. Please contact the Enact team if you are unsure about what your username or public api key are.
        """
        from .credentials_holder import CredentialsHolder

        self.enact_credentials = CredentialsHolder(username, public_api_key)

    @DEFAULT_HTTP_RETRY_POLICY
    async def _post_request(self, endpoint: str, request_details: dict):
        headers = {
            "Authorization": "Bearer " + self.enact_credentials.bearer_token,
            "Content-Type": "application/json",
            "cache-control": "no-cache",
        }

        async with httpx.AsyncClient(verify=True) as client:
            response = await client.post(endpoint, json=request_details, headers=headers)

        # check if bearer token has expired and if it has create a new one
        if response.status_code == 401 and "WWW-Authenticate" in response.headers:
            response = await self._handle_authorisation_error(endpoint, request_details, headers)

        if response.status_code != 200:
            await self._handle_error_and_get_updated_response(response)
        return response.json()

    async def _handle_error_and_get_updated_response(self, response: httpx.Response):
        if response.text != "" and "messages" in response.json():
            self._raise_exception_for_enact_error(response)
        else:
            response.raise_for_status()

    def _raise_exception_for_enact_error(self, response: httpx.Response):
        response_data = response.json()
        error_messages = response_data["messages"]
        for error_message in error_messages:
            if "errorCode" in error_message and error_message["errorCode"]:
                # An error code is present, so raise an exception with the error message
                raise httpx.HTTPStatusError(
                    f'ErrorCode: {error_message["errorCode"]}. {error_message["message"]}',
                    request=response.request,
                    response=response,
                )

    @DEFAULT_HTTP_RETRY_POLICY
    async def _handle_authorisation_error(self, endpoint: str, request_details: dict, headers: dict):
        retry_count = 0
        while retry_count < self.enact_credentials.MAX_RETRIES:
            self.enact_credentials.get_bearer_token()
            headers["Authorization"] = "Bearer " + self.enact_credentials.bearer_token

            # Retry the POST request with the new bearer token
            async with httpx.AsyncClient(verify=True) as client:
                response = await client.post(endpoint, json=request_details, headers=headers)

            if response.status_code != 401:
                # Successful response, no need to retry
                break

            retry_count += 1

        if retry_count == self.enact_credentials.MAX_RETRIES:
            raise httpx.HTTPStatusError(
                "Failed to obtain a valid bearer token after multiple attempts.", response=response
            )
        return response
