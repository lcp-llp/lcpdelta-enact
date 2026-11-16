from datetime import datetime

import pytest

from tests.integration import enact_api_helper


@pytest.mark.parametrize("kind", ["period", "day", "date_range"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_niv_evolution_public_contract(kind, asynchronous):
    day = datetime(2026, 11, 10)
    options = ["NivOutturn", "AcceptedOfferVolume"]
    args = {"period": (1, day, options), "day": (day, options), "date_range": (day, day, options)}[kind]
    method = getattr(enact_api_helper, f"get_niv_evolution_for_{kind}" + ("_async" if asynchronous else ""))
    frame = await method(*args) if asynchronous else method(*args)
    assert list(frame.columns) == ["Date", "Period", "Metric", "Timestamp", "Value"]
    assert not frame.empty
    assert set(frame["Date"]) == {"10-11-2026"}
    assert set(frame["Metric"]) == set(options)
    if kind == "period":
        assert set(frame["Period"]) == {1}
