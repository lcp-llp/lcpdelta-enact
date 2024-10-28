import lcp_delta.common.constants as constants

SERIES_DATA = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Data_V2"
SERIES_INFO = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Info"
SERIES_BY_FUEL = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Fuel"
SERIES_BY_ZONE = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Zone"
SERIES_BY_OWNER = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Owner"
SERIES_MULTI_OPTION = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/multiOption"
MULTI_SERIES_DATA = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/multipleSeriesData"
MULTI_PLANT_SERIES_DATA = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/multipleSeriesPlantData"

PLANT_INFO = f"{constants.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantInfo"
PLANT_IDS = f"{constants.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantList"

HOF = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/Data_V2"
HOF_LATEST_FORECAST = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/get_latest_forecast"

BOA = f"{constants.MAIN_BASE_URL}/EnactAPI/BOA/Data"

LEADERBOARD = f"{constants.MAIN_BASE_URL}/EnactAPI/Leaderboard/Data"

ANCILLARY = f"{constants.MAIN_BASE_URL}/EnactAPI/Ancillary/Data"

NEWS_TABLE = f"{constants.MAIN_BASE_URL}/EnactAPI/Newstable/Data"

EPEX_TRADES = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/Trades"
EPEX_TRADES_BY_CONTRACT_ID = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/TradesFromContractId"
EPEX_ORDER_BOOK = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/OrderBook"
EPEX_ORDER_BOOK_BY_CONTRACT_ID = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/OrderBookFromContractId"
EPEX_CONTRACTS = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/Contracts"

NORDPOOL_CURVES = f"{constants.SERIES_BASE_URL}/api/NordpoolBuySellCurves"

DAY_AHEAD = f"{constants.MAIN_BASE_URL}/EnactAPI/DayAhead/data"
