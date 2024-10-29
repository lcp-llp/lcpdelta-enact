import pytest
import time
from datetime import datetime
from tests.integration import flextrack_api_helper


def teardown_function():
    time.sleep(1)


expected_columns = [
    "Availability Volume - FCR (4H) - Symmetric - Austria (AT) (MW)",
    "Availability Volume - aFRR - Upward - Austria (AT) (MW)",
    "Availability Volume - aFRR - Downward - Austria (AT) (MW)",
    "Availability Price - FCR (4H) - Symmetric - Austria (AT) (EUR/MW/h)",
    "Availability Price - aFRR - Upward - Austria (AT) (EUR/MW/h)",
    "Availability Price - aFRR - Downward - Austria (AT) (EUR/MW/h)",
]


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

    assert res.index.name == "GMT Time"
    assert [column in res.columns for column in expected_columns]


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

    assert res.index.name == "GMT Time"
    assert [column in res.columns for column in expected_columns]
