import pandas as pd


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
