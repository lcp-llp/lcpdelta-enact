import pytest
import time

from datetime import datetime
from lcp_delta.common.response_objects.usage_info import UsageInfo
from tests.integration import enact_api_helper


def teardown_function():
    time.sleep(1)


def test_get_bm_data_by_period_async():
    usage = enact_api_helper.credentials_holder.get_remaining_token_count()

    assert isinstance(usage, UsageInfo)
    assert isinstance(usage.remaining_calls, int)
    assert isinstance(usage.monthly_allowance, int)
    assert isinstance(usage.last_renewed, datetime)
    assert isinstance(usage.unlimited_usage, bool)
