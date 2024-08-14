import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

MAIN_BASE_URL = "https://enactapifd.lcp.uk.com"
EPEX_BASE_URL = "https://enact-epex.azurefd.net"
SERIES_BASE_URL = "https://enact-functionapp-siteapi.azurewebsites.net"


def should_retry(exception: Exception) -> bool:
    if isinstance(exception, httpx.HTTPStatusError):
        if 500 <= exception.response.status_code < 600:  # server-side errors
            return True
        if exception.response.status_code == 408:  # request timeout
            return True
        if exception.response.status_code == 429:  # too many requests
            return True
    if isinstance(exception, httpx.RequestError):  # network errors
        return True
    return False


DEFAULT_HTTP_RETRY_POLICY = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=1, max=4),
    retry=retry_if_exception(should_retry),
)
