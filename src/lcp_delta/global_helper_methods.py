import pandas as pd
from datetime import datetime


def is_list_of_strings(lst):
    if not isinstance(lst, list):
        return False
    return all(isinstance(item, str) for item in lst)


def parse_df_datetimes(
    data: pd.DataFrame, parse_index: bool, columns_to_parse: list[str] = None, inplace=False
) -> pd.DataFrame | None:
    df = data if inplace else data.copy(deep=True)

    if parse_index:
        df.index = pd.to_datetime(df.index)
        data.sort_index(inplace=True)

    if columns_to_parse:
        for col in columns_to_parse:
            data[col] = pd.to_datetime(data[col])

    if not inplace:
        return df


def get_period_from_datetime(datetime_input: datetime) -> int:
    """Calculate the period of the day based on the hour and minute of the datetime input.

    Each period is 30 minutes long, starting from period 1 at 00:00 to 00:29.

    Args:
    - datetime_input: The datetime to calculate the period for.

    Returns:
    - int: The period of the day.
    """
    return datetime_input.hour * 2 + (datetime_input.minute // 30) + 1


def get_period(datetime_input: datetime, period: int = None) -> int:
    """Returns the correct period. If no period is specified and datetime_input is a datetime, calculate the period.

    Args:
    - datetime_input: The date or datetime object.
    - period (optional): The period. If None and datetime_input is a datetime, the period is calculated.

    Returns:
    - int: The period.

    Raises:
    - TypeError: If the period is not an integer or if no period is given and datetime_input is not a datetime type.
    """
    if period is not None:
        if not isinstance(period, int):
            raise TypeError("Please enter an integer for the period")

        return period

    elif not isinstance(datetime_input, datetime):
        raise TypeError("If no period is given, the inputted date must be of the type datetime")

    return get_period_from_datetime(datetime_input)