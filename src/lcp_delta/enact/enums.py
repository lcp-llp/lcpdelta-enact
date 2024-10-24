from enum import StrEnum


class AncillaryContractGroup(StrEnum):
    Ffr = "Ffr"
    StorDayAhead = "StorDayAhead"
    ManFr = "ManFr"
    SFfr = "SFfr"
    PositiveBalancingReserve = "PositiveBalancingReserve"
    NegativeBalancingReserve = "NegativeBalancingReserve"
    DynamicContainmentEfa = "DynamicContainmentEfa"
    DynamicContainmentEfaHF = "DynamicContainmentEfaHF"
    DynamicModerationLF = "DynamicModerationLF"
    DynamicModerationHF = "DynamicModerationHF"
    DynamicRegulationHF = "DynamicRegulationHF"