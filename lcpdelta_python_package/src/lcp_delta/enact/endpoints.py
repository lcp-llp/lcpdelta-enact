from lcp_delta.enact.loader import get_base_endpoints

base_endpoints = get_base_endpoints()

MAIN_BASE_URL = base_endpoints.MAIN_BASE_URL
SERIES_BASE_URL = base_endpoints.SERIES_BASE_URL
PUSH_SERVICE_BASE_URL = base_endpoints.PUSH_SERVICE_BASE_URL
EPEX_BASE_URL = base_endpoints.EPEX_BASE_URL

SERIES_DATA = f"{MAIN_BASE_URL}/EnactAPI/Series/Data_V2"
SERIES_INFO = f"{MAIN_BASE_URL}/EnactAPI/Series/Info"
SERIES_BY_FUEL = f"{MAIN_BASE_URL}/EnactAPI/Series/Fuel"
SERIES_BY_ZONE = f"{MAIN_BASE_URL}/EnactAPI/Series/Zone"
SERIES_BY_OWNER = f"{MAIN_BASE_URL}/EnactAPI/Series/Owner"
SERIES_MULTI_OPTION = f"{MAIN_BASE_URL}/EnactAPI/Series/multiOption"
MULTI_SERIES_DATA = f"{MAIN_BASE_URL}/EnactAPI/Series/multipleSeriesData"
MULTI_PLANT_SERIES_DATA = f"{MAIN_BASE_URL}/EnactAPI/Series/multipleSeriesPlantData"

PLANT_INFO = f"{MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantInfo"
PLANT_INFO_BY_FUEL = f"{MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantInfoByFuelType"
PLANT_IDS = f"{MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantList"

HOF = f"{MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/Data_V2"
HOF_LATEST_FORECAST = f"{MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/get_latest_forecast"

BOA = f"{MAIN_BASE_URL}/EnactAPI/BOA/Data"

LEADERBOARD_V1 = f"{MAIN_BASE_URL}/EnactAPI/Leaderboard/v1/data"
LEADERBOARD_V2 = f"{MAIN_BASE_URL}/EnactAPI/Leaderboard/v2/data"

EUROPE_INDEX_DATA = f"{SERIES_BASE_URL}/api/EuropeIndexData"
EUROPE_INDEX_DEFAULT_INDICES = f"{SERIES_BASE_URL}/api/EuropeIndexDefaultIndexInformation"
EUROPE_INDEX_INFORMATION = f"{SERIES_BASE_URL}/api/EuropeIndexInformation"
GB_INDEX_DATA = f"{SERIES_BASE_URL}/api/GbIndexData"
GB_INDEX_INFORMATION = f"{SERIES_BASE_URL}/api/GbIndexInformation"
CONTRACT_EVOLUTION = f"{SERIES_BASE_URL}/api/ContractEvolution"

ANCILLARY = f"{MAIN_BASE_URL}/EnactAPI/Ancillary/Data"

NEWS_TABLE = f"{MAIN_BASE_URL}/EnactAPI/Newstable/Data"

EPEX_TRADES = f"{EPEX_BASE_URL}/EnactAPI/Data/Trades"
EPEX_TRADES_BY_CONTRACT_ID = f"{EPEX_BASE_URL}/EnactAPI/Data/TradesFromContractId"
EPEX_ORDER_BOOK = f"{EPEX_BASE_URL}/EnactAPI/Data/OrderBook"
EPEX_ORDER_BOOK_BY_CONTRACT_ID = f"{EPEX_BASE_URL}/EnactAPI/Data/OrderBookFromContractId"
EPEX_CONTRACTS = f"{EPEX_BASE_URL}/EnactAPI/Data/Contracts"

NORDPOOL_CURVES = f"{SERIES_BASE_URL}/api/NordpoolBuySellCurves"

DAY_AHEAD = f"{MAIN_BASE_URL}/EnactAPI/DayAhead/data"

DPS = f"{PUSH_SERVICE_BASE_URL}/dataHub"
