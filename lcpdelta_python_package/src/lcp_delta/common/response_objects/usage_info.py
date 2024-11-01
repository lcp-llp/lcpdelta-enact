from datetime import datetime


class UsageInfo:
    def __init__(self, remaining_calls: int, monthly_allowance: int, last_renewed: datetime, unlimited_usage: bool):
        self.remaining_calls = remaining_calls
        self.monthly_allowance = monthly_allowance
        self.last_renewed = last_renewed
        self.unlimited_usage = unlimited_usage
