import pytest
import time
from datetime import datetime
from tests.integration import flextrack_api_helper


def teardown_function():
    time.sleep(1)


@pytest.mark.asyncio
async def test_get_exporter_data_async():
    res = await flextrack_api_helper.get_exporter_data_async(
        date_from=datetime(2022, 11, 1),
        date_to=datetime(2023, 10, 31),
        countries=["Austria"],
        products=["RegelleistungFcrProcuredFourHourly", "RegelleistungFcrProcuredDaily", "RegelleistungAfrrProcured"],
        directions=["Symmetric", "Upward", "Downward"],
        market="Availability",
        metrics=["Volume", "Price"],
        aggregation_types=["Average", "Average"],
        granularity="Monthly",
    )
    assert res is not None


def test_get_exporter_data_sync():
    res = flextrack_api_helper.get_exporter_data(
        date_from=datetime(2022, 11, 1),
        date_to=datetime(2023, 10, 31),
        countries=["Austria"],
        products=["RegelleistungFcrProcuredFourHourly", "RegelleistungFcrProcuredDaily", "RegelleistungAfrrProcured"],
        directions=["Symmetric", "Upward", "Downward"],
        market="Availability",
        metrics=["Volume", "Price"],
        aggregation_types=["Average", "Average"],
        granularity="Monthly",
    )
    assert res is not None
