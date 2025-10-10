from enum import Enum

class StrEnumMixin(str, Enum):
    """Mixin to emulate StrEnum in Python < 3.11."""
    pass

class AncillaryContracts(StrEnumMixin):
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
    DynamicRegulationLF = "DynamicRegulationLF"
