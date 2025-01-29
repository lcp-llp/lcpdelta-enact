import lcp_delta.common.constants as constants

SERIES_DATA = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/Data_V2"
SERIES_INFO = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/Info"
SERIES_BY_FUEL = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/Fuel"
SERIES_BY_ZONE = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/Zone"
SERIES_BY_OWNER = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/Owner"
SERIES_MULTI_OPTION = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/multiOption"
MULTI_SERIES_DATA = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/multipleSeriesData"
MULTI_PLANT_SERIES_DATA = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Series/multipleSeriesPlantData"

PLANT_INFO = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Plant/Data/PlantInfo"
PLANT_IDS = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Plant/Data/PlantList"

HOF = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/HistoryOfForecast/Data_V2"
HOF_LATEST_FORECAST = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/HistoryOfForecast/get_latest_forecast"

BOA = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/BOA/Data"

LEADERBOARD_V1 = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Leaderboard/v1/data"
LEADERBOARD_V2 = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Leaderboard/v2/data"

ANCILLARY = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Ancillary/Data"

NEWS_TABLE = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/Newstable/Data"

EPEX_TRADES = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/Trades"
EPEX_TRADES_BY_CONTRACT_ID = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/TradesFromContractId"
EPEX_ORDER_BOOK = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/OrderBook"
EPEX_ORDER_BOOK_BY_CONTRACT_ID = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/OrderBookFromContractId"
EPEX_CONTRACTS = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/Contracts"

NORDPOOL_CURVES = f"{constants.SERIES_BASE_URL_DEV}/api/NordpoolBuySellCurves"

DAY_AHEAD = f"{constants.MAIN_BASE_URL_DEV}/EnactAPI/DayAhead/data"
