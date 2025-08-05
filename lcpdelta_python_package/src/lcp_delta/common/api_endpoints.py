from lcp_delta.enact.loader import get_base_endpoints
import lcp_delta.common.constants as constants
from lcp_delta.common.credentials_holder import CredentialsHolder
import httpx


class EnactApiEndpoints:
    def __init__(self, credentials_holder: CredentialsHolder, bypass_frontdoor: bool = False):
        self._bypass_frontdoor = bypass_frontdoor
        self.credentials_holder = credentials_holder
        self._base_endpoints = get_base_endpoints()

        if self._bypass_frontdoor:
            self.refresh_fd_bypass()

    def refresh_fd_bypass(self) -> None:
        lookup_map = {
            "MAIN": constants.MAIN_FD_BYPASS_LOOKUP_URL,
            "EPEX": constants.EPEX_FD_BYPASS_LOOKUP_URL
        }

        for key, bypass_url in lookup_map.items():
            try:
                actual_url = self._fetch_actual_backend_url(bypass_url)
                if actual_url != "":
                    setattr(self._base_endpoints, f"{key}_BASE_URL", actual_url)
            except Exception:
                pass

    async def refresh_fd_bypass_async(self) -> None:
        lookup_map = {
            "MAIN": constants.MAIN_FD_BYPASS_LOOKUP_URL,
            "EPEX": constants.EPEX_FD_BYPASS_LOOKUP_URL
        }

        for key, bypass_url in lookup_map.items():
            try:
                actual_url = await self._fetch_actual_backend_url_async(bypass_url)
                if actual_url != "":
                    setattr(self._base_endpoints, f"{key}_BASE_URL", actual_url)
            except Exception:
                pass

    def _fetch_actual_backend_url(self, lookup_url: str) -> str:
        with httpx.Client() as client:
            response = client.get(lookup_url, timeout=15, headers=self._get_headers())
            if response.status_code == 401 and "WWW-Authenticate" in response.headers:
                self._refresh_headers()
                response = client.get(lookup_url, timeout=15, headers=self._get_headers())
            if response.status_code != 200:
                return ""
            return response.text.strip()

    async def _fetch_actual_backend_url_async(self, lookup_url: str) -> str:
        async with httpx.AsyncClient() as client:
            response = await client.get(lookup_url, timeout=15, headers=self._get_headers())
            if response.status_code == 401 and "WWW-Authenticate" in response.headers:
                self._refresh_headers()
                response = await client.get(lookup_url, timeout=15, headers=self._get_headers())
            if response.status_code != 200:
                return ""
            return response.text.strip()

    def _get_headers(self) -> dict:
        return {
            "Authorization": "Bearer " + self.credentials_holder.bearer_token,
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
        }

    def _refresh_headers(self) -> None:
        self.credentials_holder.get_bearer_token()

    @property
    def SERIES_DATA(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/Data_V2"
    @property
    def SERIES_INFO(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/Info"
    @property
    def SERIES_BY_FUEL(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/Fuel"
    @property
    def SERIES_BY_ZONE(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/Zone"
    @property
    def SERIES_BY_OWNER(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/Owner"
    @property
    def SERIES_MULTI_OPTION(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/multiOption"
    @property
    def MULTI_SERIES_DATA(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/multipleSeriesData"
    @property
    def MULTI_PLANT_SERIES_DATA(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Series/multipleSeriesPlantData"

    @property
    def PLANT_INFO(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantInfo"
    @property
    def PLANT_INFO_BY_FUEL(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantInfoByFuelType"
    @property
    def PLANT_IDS(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantList"

    @property
    def HOF(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/Data_V2"
    @property
    def HOF_LATEST_FORECAST(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/get_latest_forecast"

    @property
    def BOA(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/BOA/Data"
    @property
    def ANCILLARY(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Ancillary/Data"
    @property
    def NEWS_TABLE(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Newstable/Data"
    @property
    def DAY_AHEAD(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/DayAhead/data"

    @property
    def LEADERBOARD_V1(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Leaderboard/v1/data"
    @property
    def LEADERBOARD_V2(self): return f"{self._base_endpoints.MAIN_BASE_URL}/EnactAPI/Leaderboard/v2/data"

    @property
    def EUROPE_INDEX_DATA(self): return f"{self._base_endpoints.SERIES_BASE_URL}/api/EuropeIndexData"
    @property
    def EUROPE_INDEX_DEFAULT_INDICES(self): return f"{self._base_endpoints.SERIES_BASE_URL}/api/EuropeIndexDefaultIndexInformation"
    @property
    def EUROPE_INDEX_INFORMATION(self): return f"{self._base_endpoints.SERIES_BASE_URL}/api/EuropeIndexInformation"
    @property
    def GB_INDEX_DATA(self): return f"{self._base_endpoints.SERIES_BASE_URL}/api/GbIndexData"
    @property
    def GB_INDEX_INFORMATION(self): return f"{self._base_endpoints.SERIES_BASE_URL}/api/GbIndexInformation"
    @property
    def CONTRACT_EVOLUTION(self): return f"{self._base_endpoints.SERIES_BASE_URL}/api/ContractEvolution"
    @property
    def NORDPOOL_CURVES(self): return f"{self._base_endpoints.SERIES_BASE_URL}/api/NordpoolBuySellCurves"

    @property
    def EPEX_TRADES(self): return f"{self._base_endpoints.EPEX_BASE_URL}/EnactAPI/Data/Trades"
    @property
    def EPEX_TRADES_BY_CONTRACT_ID(self): return f"{self._base_endpoints.EPEX_BASE_URL}/EnactAPI/Data/TradesFromContractId"
    @property
    def EPEX_ORDER_BOOK(self): return f"{self._base_endpoints.EPEX_BASE_URL}/EnactAPI/Data/OrderBook"
    @property
    def EPEX_ORDER_BOOK_BY_CONTRACT_ID(self): return f"{self._base_endpoints.EPEX_BASE_URL}/EnactAPI/Data/OrderBookFromContractId"
    @property
    def EPEX_CONTRACTS(self): return f"{self._base_endpoints.EPEX_BASE_URL}/EnactAPI/Data/Contracts"

    @property
    def DPS(self): return f"{self._base_endpoints.PUSH_SERVICE_BASE_URL}/dataHub"
