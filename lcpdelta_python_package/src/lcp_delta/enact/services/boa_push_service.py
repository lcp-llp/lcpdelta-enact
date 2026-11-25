"""Decode the shared BOA wire format into cleaned public row batches."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


_PUBLIC_FIELDS = (
    "deltaId",
    "plantId",
    "bodId",
    "volume",
    "bidPrice",
    "offerPrice",
    "cost",
    "bidOnTableTypeEnum",
    "fuelType",
    "sel",
    "mzt",
    "ndb",
    "ndo",
    "ndz",
    "mnzt",
)
_BOOLEAN_FIELDS = ("isBid", "isAccepted", "soFlag", "storFlag", "cadlFlag")


def _public_row(row: dict[str, Any]) -> dict[str, Any]:
    result = {key: copy.deepcopy(row.get(key)) for key in _PUBLIC_FIELDS}
    result.update({key: row.get(key, False) == 1 for key in _BOOLEAN_FIELDS})
    coords = row.get("coords")
    if coords is None:
        result["coords"] = [0.0, 0.0]
    else:
        if not isinstance(coords, list) or len(coords) != 2:
            raise ValueError("Invalid BOA coordinates")
        try:
            values = [Decimal(str(value)) for value in coords]
            if not all(value.is_finite() for value in values):
                raise ValueError("Invalid BOA coordinates")
            result["coords"] = [float(value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)) for value in values]
        except InvalidOperation as error:
            raise ValueError("Invalid BOA coordinates") from error
    result["additionalBsadData"] = None
    bsad = row.get("additionalBsadData")
    if row.get("fuelType") == "BSAD" and bsad is not None:
        if not isinstance(bsad, dict):
            raise ValueError("Invalid BOA BSAD metadata")
        result["additionalBsadData"] = {
            "plantId": bsad.get("assetId"),
            "partyName": bsad.get("partyId"),
            "fuelType": bsad.get("fuelType"),
        }
    result["acceptedTime"] = row.get("timeAccepted")
    return result


@dataclass(frozen=True)
class BoaPush:
    """One cleaned batch; revision is source metadata, not a delivery sequence.

    Changed collections contain complete public rows to insert/replace; removed
    collections contain public rows to remove. Identity includes settlement date,
    period, accepted/on-table collection, bid/offer side and ``deltaId``.
    Predicted rows are ignored in every collection, including removals.
    """

    date: date
    period: int
    revision: int
    accepted_bids: tuple[dict[str, Any], ...]
    accepted_offers: tuple[dict[str, Any], ...]
    removed_accepted_bids: tuple[dict[str, Any], ...]
    removed_accepted_offers: tuple[dict[str, Any], ...]
    table_bids: tuple[dict[str, Any], ...]
    table_offers: tuple[dict[str, Any], ...]
    removed_table_bids: tuple[dict[str, Any], ...]
    removed_table_offers: tuple[dict[str, Any], ...]

    @classmethod
    def from_payload(cls, payload: Any) -> BoaPush:
        """Project a shared batch without fetching data or retaining row history.

        Frontend reloadRequired and previousRevision are deliberately ignored.
        """
        while isinstance(payload, (list, tuple)) and len(payload) == 1:
            payload = payload[0]
        if not isinstance(payload, dict):
            raise ValueError("Expected a BOA object")
        date_period, item = payload["datePeriod"], payload["item"]
        if not isinstance(date_period, dict) or not isinstance(item, dict):
            raise ValueError("Invalid BOA envelope")
        settlement_date = datetime.fromisoformat(date_period["date"]).date()
        period, revision = date_period["period"], item["revision"]
        if type(period) is not int or not 1 <= period <= 50:
            raise ValueError("Invalid settlement period")
        if type(revision) is not int or revision < 0:
            raise ValueError("Invalid source revision")

        def rows(key):
            values = item[key]
            if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
                raise ValueError("Invalid BOA row collection")
            actual = [row for row in values if not row.get("theoretical") and not row.get("nonBmPrediction")]
            if any(not isinstance(row.get("deltaId"), str) or not row["deltaId"] for row in actual):
                raise ValueError("Invalid BOA row identity")
            return tuple(_public_row(row) for row in actual)

        return cls(
            date=settlement_date,
            period=period,
            revision=revision,
            accepted_bids=rows("acceptedBids"),
            accepted_offers=rows("acceptedOffers"),
            removed_accepted_bids=rows("removedAcceptedBids"),
            removed_accepted_offers=rows("removedAcceptedOffers"),
            table_bids=rows("tableBids"),
            table_offers=rows("tableOffers"),
            removed_table_bids=rows("removedTableBids"),
            removed_table_offers=rows("removedTableOffers"),
        )
