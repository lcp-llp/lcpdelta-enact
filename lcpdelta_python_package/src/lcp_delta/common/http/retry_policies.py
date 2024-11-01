import httpx

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


def should_retry_default(exception: Exception) -> bool:
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


DEFAULT_RETRY_POLICY = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=1, max=4),
    retry=retry_if_exception(should_retry_default),
)


def is_http_unauthorised(exception: Exception) -> bool:
    return isinstance(exception, httpx.HTTPStatusError) and exception.response.status_code == 401


UNAUTHORISED_INCLUSIVE_RETRY_POLICY = retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=0.5, min=1, max=4),
    retry=retry_if_exception(should_retry_default) | retry_if_exception(is_http_unauthorised),
)
