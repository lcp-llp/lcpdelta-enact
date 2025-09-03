import pandas as pd
from datetime import datetime
from typing import Union

from lcp_delta.global_helpers import convert_datetime_to_iso
from lcp_delta.common import APIHelperBase
from lcp_delta.enact.helpers import get_month_name
from lcp_delta.enact.services import ancillary_service
from lcp_delta.enact.services import contract_evolution_service
from lcp_delta.enact.services import bm_service
from lcp_delta.enact.services import day_ahead_service
from lcp_delta.enact.services import epex_service
from lcp_delta.enact.services import hof_service
from lcp_delta.enact.services import leaderboard_service
from lcp_delta.enact.services import index_service
from lcp_delta.enact.services import news_table_service
from lcp_delta.enact.services import nordpool_service
from lcp_delta.enact.services import plant_service
from lcp_delta.enact.services import series_service

class APIHelper(APIHelperBase):
    def _make_series_request(
        self,
        series_id: str,
        date_from: str,
        date_to: str,
        country_id: str,
        option_id: list[str],
        half_hourly_average: bool,
        endpoint: str,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame | dict:
        """Makes a request to the series endpoints.

        Returns:
             Response: A dictionary or pandas DataFrame containing the series data.
        """
        request_body = series_service.generate_series_data_request(
            series_id,
            date_from,
            date_to,
            country_id,
            option_id,
            half_hourly_average,
            request_time_zone_id,
            time_zone_id,
        )
        response = self._post_request(endpoint, request_body)
        return series_service.process_series_data_response(response, parse_datetimes)

    async def _make_series_request_async(
        self,
        series_id: str,
        date_from: str,
        date_to: str,
        country_id: str,
        option_id: list[str],
        half_hourly_average: bool,
        endpoint: str,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame | dict:
        """An asynchronous version of `_make_series_request`."""
        request_body = series_service.generate_series_data_request(
            series_id,
            date_from,
            date_to,
            country_id,
            option_id,
            half_hourly_average,
            request_time_zone_id,
            time_zone_id,
        )
        response = await self._post_request_async(endpoint, request_body)
        return series_service.process_series_data_response(response, parse_datetimes)

    def get_series_data(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: list[str] | None = None,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Gets series data from Enact.

        Args:
            series_id `str`: The Enact series ID.

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_id `list[str]`: The Enact option IDs, if options are applicable, e.g. ["Coal"]. The input is a list as some series require multiple options, e.g. ["Z1", "Median"].  You cannot use the list to get the values for multiple options, e.g. ["Coal", "Wind"], you will need to make two separate requests to get values for different options.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        return self._make_series_request(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_id,
            half_hourly_average,
            self.endpoints.SERIES_DATA,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    async def get_series_data_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: list[str] | None = None,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_series_data`."""
        return await self._make_series_request_async(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_id,
            half_hourly_average,
            self.endpoints.SERIES_DATA,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    def get_series_info(self, series_id: str, country_id: str | None = None) -> dict:
        """Gets information about a specific Enact series.

        Args:
            series_id `str`: The series ID.
            country_id `str` (optional): The country ID (defaults to None). If not provided, data will be returned for the first country available for this series.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            An object containing information about the series. This includes series name, countries with data for that series, options related to the series, whether or not the series has historical data, and whether
            or not the series has historical forecasts.
        """
        request_body = series_service.generate_series_info_request(series_id, country_id)
        return self._post_request(self.endpoints.SERIES_INFO, request_body)

    async def get_series_info_async(self, series_id: str, country_id: str | None = None) -> dict:
        """An asynchronous version of `get_series_info`."""
        request_body = series_service.generate_series_info_request(series_id, country_id)
        return await self._post_request_async(self.endpoints.SERIES_INFO, request_body)

    def get_series_by_fuel(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str],
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Gets plant series data for a given fuel type.

        Args:
            series_id `str`: The Enact series ID (must be a plant series).

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_ids `list[str]`: The fuel option for the request with any other options required for the series, e.g. ["Coal",...]. The input is a list as some plant series require multiple options, e.g. ["Coal", "Offer"].  You cannot use the list to get the values for multiple options, e.g. ["Coal", "Wind"], you will need to make two separate requests to get values for different options.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        return self._make_series_request(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,  # fuel and other options
            half_hourly_average,
            self.endpoints.SERIES_BY_FUEL,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    async def get_series_by_fuel_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str],
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_series_by_fuel`."""
        return await self._make_series_request_async(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,  # fuel and other options
            half_hourly_average,
            self.endpoints.SERIES_BY_FUEL,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    def get_series_by_zone(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str],
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Get plant series data for a given zone.

        Args:
            series_id `str`: The Enact series ID (must be a plant series).

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_ids `list[str]`: The zone option for the request with any other options required for the series, e.g. ["Z1",...]. The input is a list as some plant series require multiple options, e.g. ["Z1", "Offer"].  You cannot use the list to get the values for multiple options, e.g. ["Z1", "Z2"], you will need to make two separate requests to get values for different options.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        return self._make_series_request(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,  # zone and other options
            half_hourly_average,
            self.endpoints.SERIES_BY_ZONE,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    async def get_series_by_zone_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str],
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_series_by_zone`."""
        return await self._make_series_request_async(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,  # zone and other options
            half_hourly_average,
            self.endpoints.SERIES_BY_ZONE,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    def get_series_by_owner(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str],
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Get plant series data for a given owner.

        Args:
            series_id `str`: The Enact series ID (must be a plant series).

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_ids `list[str]`: The owner option for the request with any other options required for the series, e.g. ["Adela Energy",...]. The input is a list as some plant series require multiple options, e.g. ["Adela Energy", "Offer"].  You cannot use the list to get the values for multiple options, e.g. ["Adela Energy", "SSE"], you will need to make two separate requests to get values for different options.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        return self._make_series_request(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,  # owner and other options
            half_hourly_average,
            self.endpoints.SERIES_BY_OWNER,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    async def get_series_by_owner_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str],
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_series_by_owner`."""
        return await self._make_series_request_async(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,  # owner and other options
            half_hourly_average,
            self.endpoints.SERIES_BY_OWNER,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    def get_series_multi_option(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: list[str] | None = None,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Get series data for a specific non-plant series with multiple options available.

        Args:
            series_id `str`: The Enact series ID (must not be a plant series).

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_id `list[str]`: The option IDs, e.g. ["Coal", "Wind"]. If left empty all possible options will be returned.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that the arguments required for specific enact data can be found on the site.

        Returns:
            Response: The response object containing the series data.
        """
        return self._make_series_request(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_id,
            half_hourly_average,
            self.endpoints.SERIES_MULTI_OPTION,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    async def get_series_multi_option_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: list[str] | None = None,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_series_multi_option`."""
        return await self._make_series_request_async(
            series_id,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_id,
            half_hourly_average,
            self.endpoints.SERIES_MULTI_OPTION,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    def get_multi_series_data(
        self,
        series_ids: list[str],
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str] | None = None,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Get data for multiple non-plant series.

        Args:
            series_ids `list[str]`: A list of Enact series IDs (must not be a plant series).

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_ids `list[str]` (optional): The option IDs, e.g. ["Coal"]. The same option_ids will be used for all series_ids. If left empty all possible options will be returned.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that the arguments required for specific enact data can be found on the site.

        Returns:
            Response: The response object containing the series data.
        """
        request_body = series_service.generate_multi_series_data_request(
            series_ids,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,
            half_hourly_average,
            request_time_zone_id,
            time_zone_id,
        )
        response = self._post_request(self.endpoints.MULTI_SERIES_DATA, request_body)
        return series_service.process_series_data_response(response, parse_datetimes)

    async def get_multi_series_data_async(
        self,
        series_ids: list[str],
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_ids: list[str] | None = None,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_multi_series_data`"""
        request_body = series_service.generate_multi_series_data_request(
            series_ids,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,
            half_hourly_average,
            request_time_zone_id,
            time_zone_id,
        )
        response = await self._post_request_async(self.endpoints.MULTI_SERIES_DATA, request_body)
        return series_service.process_series_data_response(response, parse_datetimes)

    def get_multi_plant_series_data(
        self,
        series_ids: list[str],
        option_ids: list[str],
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Get series data for multiple plant series.

        Args:
            series_ids `list[str]`: A list of Enact series IDs.

            option_ids `list[str]`: The plant IDs corresponding to each series index requested, e.g. ["E_BHOLB-1", "T_RYHPS-1"].

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that the arguments required for specific enact data can be found on the site.

        Returns:
            Response: The response object containing the series data.
        """
        request_body = series_service.generate_multi_series_data_request(
            series_ids,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,
            half_hourly_average,
            request_time_zone_id,
            time_zone_id,
        )
        response = self._post_request(self.endpoints.MULTI_PLANT_SERIES_DATA, request_body)
        return series_service.process_series_data_response(response, parse_datetimes)

    async def get_multi_plant_series_data_async(
        self,
        series_ids: list[str],
        option_ids: list[str],
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_multi_plant_series_data`"""
        request_body = series_service.generate_multi_series_data_request(
            series_ids,
            convert_datetime_to_iso(date_from),
            convert_datetime_to_iso(date_to),
            country_id,
            option_ids,
            half_hourly_average,
            request_time_zone_id,
            time_zone_id,
        )
        response = await self._post_request_async(self.endpoints.MULTI_PLANT_SERIES_DATA, request_body)
        return series_service.process_series_data_response(response, parse_datetimes)

    def get_plant_details_by_id(self, plant_id: str) -> dict:
        """Get details of a plant based on the plant ID.

        Args:
            plant_id `str`: The Enact plant ID.
        """
        request_body = plant_service.generate_plant_request(plant_id)
        return self._post_request(self.endpoints.PLANT_INFO, request_body)

    async def get_plant_details_by_id_async(self, plant_id: str) -> dict:
        """An asynchronous version of `get_pant_details_by_id`."""
        request_body = plant_service.generate_plant_request(plant_id)
        return await self._post_request_async(self.endpoints.PLANT_INFO, request_body)

    def get_plant_details_by_fuel(self, fuel: str) -> dict:
        """Get details of all plants of a particular fuel.

        Args:
            fuel `str`: The fuel.
        """
        request_body = plant_service.generate_fuel_request(fuel)
        response =  self._post_request(self.endpoints.PLANT_INFO_BY_FUEL, request_body)
        return plant_service.process_country_fuel_response(response)

    async def get_plant_details_by_fuel_async(self, fuel: str) -> dict:
        """An asynchronous version of `get_pant_details_by_id`."""
        request_body = plant_service.generate_fuel_request(fuel)
        response = await self._post_request_async(self.endpoints.PLANT_INFO_BY_FUEL, request_body)
        return plant_service.process_country_fuel_response(response)

    def get_plants_by_fuel_and_country(self, fuel_id: str, country_id: str) -> list[str]:
        """Get a list of plants for a given fuel and country.

        Args:
            fuel_id `str`: The fuel ID.
            country_id `str` (optional): The country ID. Defaults to "Gb".

        Returns:
            Response: The response object containing the plant data.
        """
        request_body = plant_service.generate_country_fuel_request(country_id, fuel_id)
        response = self._post_request(self.endpoints.PLANT_IDS, request_body)
        return plant_service.process_country_fuel_response(response)

    async def get_plants_by_fuel_and_country_async(self, fuel_id: str, country_id: str) -> list[str]:
        """An asynchronous version of `get_plants_by_fuel_and_country`."""
        request_body = plant_service.generate_country_fuel_request(country_id, fuel_id)
        response = await self._post_request_async(self.endpoints.PLANT_IDS, request_body)
        return plant_service.process_country_fuel_response(response)

    def get_history_of_forecast_for_given_date(
        self, series_id: str, date: datetime, country_id: str, option_id: str | None = None
    ) -> pd.DataFrame:
        """Gets the history (all iterations) of a series forecast for a given date.

        Args:
            series_id `str`: The Enact series ID.

            date `datetime.date`: The date to request forecasts for.

            country_id `str` (optional): This Enact country ID. Defaults to "Gb".

            option_id `list[str]` (optional): The Enact option ID, if an option is applicable. Defaults to None. The input is a list as some series require multiple options, e.g. ["Coal", "Offer"].  You cannot use the list to get the values for multiple options, e.g. ["Coal", "Wind"], you will need to make two separate requests to get values for different options.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A pandas DataFrame holding all data for the requested series on the requested date.
            The first row will provide all the dates we have a forecast iteration for.
            All other rows correspond to the data-points at each value of the first array.
        """
        request_body = hof_service.generate_single_date_request(series_id, date, country_id, option_id)
        response = self._post_request(self.endpoints.HOF, request_body)
        return hof_service.process_single_date_response(response)

    async def get_history_of_forecast_for_given_date_async(
        self, series_id: str, date: datetime, country_id: str, option_id: str | None = None
    ) -> pd.DataFrame:
        """An asynchronous version of `get_history_of_forecast_for_given_date`."""
        request_body = hof_service.generate_single_date_request(series_id, date, country_id, option_id)
        response = await self._post_request_async(self.endpoints.HOF, request_body)
        return hof_service.process_single_date_response(response)

    def get_history_of_forecast_for_date_range(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Gets the history of a forecast for a given date.

        Args:
            series_id `str`: The Enact series ID.

            date_from `datetime.datetime`: The start date to request forecasts for.

            date_to `datetime.datetime`: The end date to request forecasts for.

            country_id `str` (optional): This Enact country ID. Defaults to "Gb".

            option_id `list[str]` (optional): The Enact option IDs, if an options are applicable. Defaults to None. The input is a list as some series require multiple options, e.g. ["Coal", "Offer"].  You cannot use the list to get the values for multiple options, e.g. ["Coal", "Wind"], you will need to make two separate requests to get values for different options.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A dictionary of strings and pandas DataFrames holding all data for the requested series on the requested date.
            The first row will provide all the dates we have a forecast iteration for.
            All other rows correspond to the data-points at each value of the first array.
        """
        response_body = hof_service.generate_date_range_request(series_id, date_from, date_to, country_id, option_id)
        response = self._post_request(self.endpoints.HOF, response_body)
        return hof_service.process_date_range_response(response)

    async def get_history_of_forecast_for_date_range_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """An asynchronous version of `get_history_of_forecast_for_date_range_async`."""
        response_body = hof_service.generate_date_range_request(series_id, date_from, date_to, country_id, option_id)
        response = await self._post_request_async(self.endpoints.HOF, response_body)
        return hof_service.process_date_range_response(response)

    def get_latest_forecast_generated_at_given_time(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        forecast_as_of: datetime,
        option_id: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Gets the latest forecast generated prior to the given 'forecast_as_of' datetime.

        Args:
            series_id `str`: The Enact series ID.

            date_from `datetime.datetime`: The start date to request forecasts for.

            date_to `datetime.datetime`: The end date to request forecasts for.

            country_id `str` (optional): This Enact country ID. Defaults to "Gb".

            forecast_as_of `datetime.datetime`: The date you want the latest forecast generated for.

            option_id `list[str]` (optional): The Enact option IDs, if an options are applicable. Defaults to None. The input is a list as some series require multiple options, e.g. ["Coal", "Offer"].  You cannot use the list to get the values for multiple options, e.g. ["Coal", "Wind"], you will need to make two separate requests to get values for different options.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A dictionary of string and pandas DataFrames, holding the latest forecast generated to the given 'forecast_as_of' datetime for the range of dates requested.
            The keys are the datetime strings of each of these dates. The first row of each DataFrame will provide the date we have a forecast iteration for, which will be the latest generated
            forecast before the given 'forecast_as_of' datetime. All other rows correspond to the data-points at each value of the first array.
        """
        request_body = hof_service.generate_latest_forecast_request(
            series_id, date_from, date_to, country_id, forecast_as_of, option_id
        )
        response = self._post_request(self.endpoints.HOF_LATEST_FORECAST, request_body)
        return hof_service.process_latest_forecast_response(response)

    async def get_latest_forecast_generated_at_given_time_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        forecast_as_of: datetime,
        option_id: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """An asynchronous version of `get_latest_forecast_generated_at_given_time`."""
        request_body = hof_service.generate_latest_forecast_request(
            series_id, date_from, date_to, country_id, forecast_as_of, option_id
        )
        response = await self._post_request_async(self.endpoints.HOF_LATEST_FORECAST, request_body)
        return hof_service.process_latest_forecast_response(response)

    def get_bm_data_by_period(
        self, date: datetime, period: int = None, include_accepted_times: bool = False
    ) -> pd.DataFrame:
        """Gets BM (Balancing Mechanism) data for a specific date and period.

        Args:
            date `datetime.datetime`: The date to request BOD data for.

            period `int` (optional): The period for which to retrieve the BM data. If None and date input is of type datetime, the period is calculated (rounded down to the nearest half-hour).

            include_accepted_times `bool`: Choose whether object include BOA accepted times or not

        Returns:
            Response: A pandas DataFrame containing the BM data.

        Raises:
            `TypeError`: If the period is not an integer or if no period is given and date is not of type datetime.
        """
        request_body = bm_service.generate_by_period_request(date, period, include_accepted_times)
        response = self._post_request(self.endpoints.BOA, request_body, long_timeout=True)
        return bm_service.process_by_period_response(response)
    
    async def get_bm_data_by_period_async(
        self, date: datetime, period: int = None, include_accepted_times: bool = False
    ) -> pd.DataFrame:
        """An asynchronous version of `get_bm_data_by_period`."""
        request_body = bm_service.generate_by_period_request(date, period, include_accepted_times)
        response = await self._post_request_async(self.endpoints.BOA, request_body, long_timeout=True)
        return bm_service.process_by_period_response(response)
    
    def get_bm_data_by_day(
        self,
        date: datetime,
        option: str = "all",
        search_string: str | None = None,
        include_accepted_times: bool = True,
        include_on_table: bool = True
    ) -> pd.DataFrame:
        """
        same as get_bm_data_by_search except include_accepted_times and include_on_table default is true
        """
        request_body = bm_service.generate_by_search_request(date, option, search_string, include_accepted_times, include_on_table)
        response = self._get_request(self.endpoints.BOA_DAILY, request_body, long_timeout=True)
        return bm_service.process_by_search_response(response)
    
    async def get_bm_data_by_day_async(
        self,
        date: datetime,
        option: str = "all",
        search_string: str | None = None,
        include_accepted_times: bool = True,
        include_on_table: bool = True
    ) -> pd.DataFrame:
        """An asynchronous version of `get_bm_data_by_day`."""
        request_body = bm_service.generate_by_search_request(date, option, search_string, include_accepted_times, include_on_table)
        response = await self._get_request_async(self.endpoints.BOA_DAILY, request_body, long_timeout=True)
        return bm_service.process_by_search_response(response)

    def get_bm_data_by_day_range(
        self,
        start_date: datetime,
        end_date: datetime,
        option: str = "all",
        search_string: str | None = None,
        include_accepted_times: bool = True,
        include_on_table: bool = True
    ) -> pd.DataFrame:
        """Gets BM (Balancing Mechanism) data for a specific date range and search criteria.

            Args:
            start_date `datetime.datetime`: The date to request BOD data for.

            end_date `datetime.datetime`: The date to request BOD data for.

            option `str`: The search option; can be set to "plant", "fuel", or "all".

            search_string `str`: The search string to match against the BM data. If option is "plant", this allows you to filter BOA actions by BMU ID (e.g. "CARR" for all Carrington units).
                                If option is "fuel", this allows you to filter BOA actions by fuel type (e.g. "Coal"). If Option is "all", this must not be passed as an argument.

            include_accepted_times `bool`: Determine whether the returned object includes a column for accepted times in the response object. Defaults to True.

            include_on_table 'bool': Option to return all data on table, defaults to True.
        Returns:
            Response: A pandas DataFrame containing the BM data.
        """
        all_data = []
        cursor = None
        headers = None
        more_pages = True
        while more_pages:
            request_body = bm_service.generate_date_range_request(start_date, end_date, include_accepted_times, option, search_string, include_on_table, cursor)
            response = self._get_request(self.endpoints.BOA_DAY_RANGE, request_body, long_timeout=True)
            page_data = response.get("data", {})
            rows = page_data.get("data", [])

            if not rows:
                print(f"No data for date: {cursor}")
            else: 
                if headers is None:
                    headers = rows[0]
                all_data.extend(rows[1:])
                
            cursor = response.get("nextCursor")
            if not cursor:
                more_pages = False
        return pd.DataFrame(all_data, columns=headers)

    async def get_bm_data_by_day_range_async(
        self,
        start_date: datetime,
        end_date: datetime,
        option: str = "all",
        search_string: str | None = None,
        include_accepted_times: bool = True,
        include_on_table: bool = True
    ) -> pd.DataFrame:
        """Asynchronous version of 'get_bm_data_by_day_range'."""
        all_data = []
        cursor = None
        headers = None
        more_pages = True
        while more_pages:
            request_body = bm_service.generate_date_range_request(start_date, end_date, include_accepted_times, option, search_string, include_on_table, cursor)
            response = await self._get_request_async(self.endpoints.BOA_DAY_RANGE, request_body, long_timeout=True)
            page_data = response.get("data", {})
            rows = page_data.get("data", [])

            if not rows:
                print(f"No data for date: {cursor}")
            else:
                if headers is None:
                    headers = rows[0]
                all_data.extend(rows[1:])

            cursor = response.get("nextCursor")
            if not cursor:
                more_pages = False
        return pd.DataFrame(all_data, columns=headers)
    
    def get_bm_data_by_search(
        self,
        date: datetime,
        option: str = "all",
        search_string: str | None = None,
        include_accepted_times: bool = False,
        include_on_table: bool = False
    ) -> pd.DataFrame:
        """Gets BM (Balancing Mechanism) data for a specific date and search criteria.

        Args:
            date `datetime.datetime`: The date to request BOD data for.

            option `str`: The search option; can be set to "plant", "fuel", or "all".

            search_string `str`: The search string to match against the BM data. If option is "plant", this allows you to filter BOA actions by BMU ID (e.g. "CARR" for all Carrington units).
                                If option is "fuel", this allows you to filter BOA actions by fuel type (e.g. "Coal"). If Option is "all", this must not be passed as an argument.

            include_accepted_times `bool`: Determine whether the returned object includes a column for accepted times in the response object. Defaults to false.

            include_on_table 'bool': Option to return all data on table, defaults to false.
        Returns:
            Response: A pandas DataFrame containing the BM data.
        """
        request_body = bm_service.generate_by_search_request(date, option, search_string, include_accepted_times, include_on_table)
        response = self._get_request(self.endpoints.BOA_DAILY, request_body, long_timeout=True)
        return bm_service.process_by_search_response(response)

    async def get_bm_data_by_search_async(
        self,
        date: datetime,
        option: str = "all",
        search_string: str | None = None,
        include_accepted_times: bool = False,
        include_on_table: bool = False
    ) -> pd.DataFrame:
        """An asynchronous version of `get_bm_data_by_search`."""
        request_body = bm_service.generate_by_search_request(date, option, search_string, include_accepted_times, include_on_table)
        response = await self._get_request_async(self.endpoints.BOA_DAILY, request_body, long_timeout=True)
        return bm_service.process_by_search_response(response)

    def get_leaderboard_data_legacy(
        self,
        date_from: datetime,
        date_to: datetime,
        type="Plant",
        revenue_metric="PoundPerMwPerH",
        market_price_assumption="WeightedAverageDayAheadPrice",
        gas_price_assumption="DayAheadForward",
        include_capacity_market_revenues=False,
    ) -> pd.DataFrame:
        """Gets leaderboard data for a given date range.

        Args:
            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Set equal to the start date to return data for a given day.

            type `str`: The type of leaderboard to be requested; "Plant", "Owner" or "Battery".

            revenue_metric `str` (optional): The unit which revenues will be measured in for the leaderboard; "Pound" or "PoundPerMwPerH" (default).

            market_price_assumption `str` (optional): The price assumption for wholesale revenues on the leaderboard.
                Possible options are: "WeightedAverageDayAheadPrice" (default), "EpexDayAheadPrice", "NordpoolDayAheadPrice", "IntradayPrice" or "BestPrice".

            gas_price_assumption `str` (optional): The gas price assumption; "DayAheadForward" (default), "DayAheadSpot", "WithinDaySpot" or "CheapestPrice".

            include_capacity_market_revenues `bool` (optional): Shows the Capacity Market revenue column and factors them into net revenues. Defaults to false.
        """
        request_body = leaderboard_service.generate_request_v1(
            date_from,
            date_to,
            type,
            revenue_metric,
            market_price_assumption,
            gas_price_assumption,
            include_capacity_market_revenues,
        )
        response = self._post_request(self.endpoints.LEADERBOARD_V1, request_body)
        return leaderboard_service.process_response(response, type)

    async def get_leaderboard_data_legacy_async(
        self,
        date_from: datetime,
        date_to: datetime,
        type="Plant",
        revenue_metric="PoundPerMwPerH",
        market_price_assumption="WeightedAverageDayAheadPrice",
        gas_price_assumption="DayAheadForward",
        include_capacity_market_revenues=False,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_leaderboard_data_legacy`."""
        request_body = leaderboard_service.generate_request_v1(
            date_from,
            date_to,
            type,
            revenue_metric,
            market_price_assumption,
            gas_price_assumption,
            include_capacity_market_revenues,
        )
        response = await self._post_request_async(self.endpoints.LEADERBOARD_V1, request_body)
        return leaderboard_service.process_response(response, type)

    def get_leaderboard_data(
        self,
        date_from: datetime,
        date_to: datetime,
        type="Plant",
        revenue_metric="PoundPerMwPerH",
        market_price_assumption="WeightedAverageDayAheadPrice",
        gas_price_assumption="DayAheadForward",
        include_capacity_market_revenues=False,
        ancillary_profit_aggregation="FrequencyAndReserve",
        group_dx=False,
        aggregate=None,
        show_co_located_fuels=False,
        account_for_availability_in_normalisation=False,
        fuels=None,
        include_imbalance=False,
        include_estimated_charging_cost=False,
        include_fpnflagoff_wholesale=False,
        charging_cost_price="IntradayPrice",
        charging_cost_assumption="PreviousEFABlock",
        non_delivery_split_out = "Show"
    ) -> pd.DataFrame:
        """Gets leaderboard data for a given date range.

        Args:
            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Set equal to the start date to return data for a given day.

            type `str`: The type of leaderboard to be requested; "Plant", "Owner" or "Battery".

            revenue_metric `str` (optional): The unit which revenues will be measured in for the leaderboard; "Pound" or "PoundPerMwPerH" (default).

            market_price_assumption `str` (optional): The price assumption for wholesale revenues on the leaderboard.
                Possible options are: "WeightedAverageDayAheadPrice" (default), "EpexDayAheadPrice", "NordpoolDayAheadPrice", "IntradayPrice" or "BestPrice".

            gas_price_assumption `str` (optional): The gas price assumption; "DayAheadForward" (default), "DayAheadSpot", "WithinDaySpot" or "CheapestPrice".

            include_capacity_market_revenues `bool` (optional): Shows the Capacity Market revenue column and factors them into net revenues. Defaults to false.

            ancillary_profit_aggregation `str` (optional): The aggregation option for ancillary profits. Options are: "FrequencyAndReserve", "ByProduct", and "ByDirection". Defaults to "FrequencyAndReserve".

            group_dx `bool` (optional): When set to true, DC, DR, and DL profits will be grouped into "Dx". Defaults to False.

            aggregate (optional, str): Aggregation level ("Day", "Week", "Month"). Defaults to None. For the given date range, data is aggregated by the specified period (e.g., "Month" splits the original date range into months creating rows in the dataframe for each month). An aggregate column indicates the start date of each aggregation.

            show_co_located_fuels `bool` (optional): When set to true, a column will show the fuel types of co-located plants. Defaults to False.

            account_for_availability_in_normalisation `bool` (optional): When set to true, the normalisation process will account for plant availability. Defaults to False.

            fuels (optional, array_str): List of fuel types to include. Leave empty to get all fuels back.

            include_imbalance `str` (optional): Set to "False" (default) to exclude imbalance payments for non-BM and secondary BMUs.

            include_estimated_charging_cost `str` (optional): Set to "False" (default) to exclude estimated charging/discharging costs for non-BM and secondary BMUs.

            include_fpnflagoff_wholesale `str` (optional): Set to "False" (default) to exclude estimated wholesale revenue for BM (No FPN) assets.

            charging_cost_price `str` (optional): The price assumption using for the estimated charging/discharging costs of non-BM and secondary BMUs. Options are: 'WeightedAverageDayAheadPrice', 'EpexDayAheadPrice', 'NordpoolDayAheadPrice', 'IntradayPrice' (default) or 'SystemPrice'.

            charging_cost_assumption `str` (optional): The charging cost assumption used for the estimated charging/discharging costs for non-BM and secondary BMUs. Options are: 'PreviousEFABlock' (default), 'OptimalPricePrev12Hours' and 'CurrentSp'.

            non_delivery_split_out `str` (optional): Non-delivery column option. Options are: 'Hide', 'Show', 'Absorb'."""
        request_body = leaderboard_service.generate_request_v2(
            date_from,
            date_to,
            type,
            revenue_metric,
            market_price_assumption,
            gas_price_assumption,
            include_capacity_market_revenues,
            ancillary_profit_aggregation,
            group_dx,
            aggregate,
            show_co_located_fuels,
            account_for_availability_in_normalisation,
            fuels,
            include_imbalance,
            include_estimated_charging_cost,
            include_fpnflagoff_wholesale,
            charging_cost_price,
            charging_cost_assumption,
            non_delivery_split_out
        )
        response = self._post_request(self.endpoints.LEADERBOARD_V2, request_body)
        return leaderboard_service.process_response(response, type)

    async def get_leaderboard_data_async(
        self,
        date_from: datetime,
        date_to: datetime,
        type="Plant",
        revenue_metric="PoundPerMwPerH",
        market_price_assumption="WeightedAverageDayAheadPrice",
        gas_price_assumption="DayAheadForward",
        include_capacity_market_revenues=False,
        ancillary_profit_aggregation="FrequencyAndReserve",
        group_dx=False,
        aggregate=None,
        show_co_located_fuels=False,
        account_for_availability_in_normalisation=False,
        fuels=None,
        include_imbalance=False,
        include_estimated_charging_cost=False,
        include_fpnflagoff_wholesale=False,
        charging_cost_price="IntradayPrice",
        charging_cost_assumption="PreviousEFABlock",
        non_delivery_split_out="Show"
    ) -> pd.DataFrame:
        """An asynchronous version of `get_leaderboard_data`."""
        request_body = leaderboard_service.generate_request_v2(
            date_from,
            date_to,
            type,
            revenue_metric,
            market_price_assumption,
            gas_price_assumption,
            include_capacity_market_revenues,
            ancillary_profit_aggregation,
            group_dx,
            aggregate,
            show_co_located_fuels,
            account_for_availability_in_normalisation,
            fuels,
            include_imbalance,
            include_estimated_charging_cost,
            include_fpnflagoff_wholesale,
            charging_cost_price,
            charging_cost_assumption,
            non_delivery_split_out
        )
        response = await self._post_request_async(self.endpoints.LEADERBOARD_V2, request_body)
        return leaderboard_service.process_response(response, type)


    def get_gb_index_information(
        self,
        index_id: str,
        ) -> pd.DataFrame:
        """ Get the defining information of the index with the given ID.

         Args:
            index_id `str`: The index ID denoting which index to get data for. Index IDs can be found on the GB Index page on Enact, by clicking the ID icon next to an index."""

        request_body = index_service.generate_index_info_request(
            index_id,
        )

        response = self._post_request(self.endpoints.GB_INDEX_INFORMATION, request_body)
        return index_service.process_index_info_response(response)

    async def get_gb_index_information_async(
        self,
        index_id: str,
        ) -> pd.DataFrame:
        """An asynchronous version of `get_gb_index_information`."""
        request_body = index_service.generate_index_info_request(
            index_id,
        )

        response = await self._post_request_async(self.endpoints.GB_INDEX_INFORMATION, request_body)
        return index_service.process_index_info_response(response)

    def get_gb_index_data(
        self,
        date_from: datetime,
        date_to: datetime,
        index_id: str,
        normalisation ="PoundPerKwPerYear",
        granularity ="Week",
        show_profit = "false",
        gas_price_assumption = "DayAheadForward",
        market_price_assumption = "WeightedAverageDayAheadPrice",
        account_for_availability_in_normalisation = "false",
        include_wholesale_split = "false",
        bm_split_out_option = None,
        ancillary_revenue_type = "ByProduct",
        group_dx = "false",
        include_capacity_market = "true",
        include_non_delivery_charges = "true",
        include_imbalance= "false",
        include_estimated_charging_cost= "false",
        include_fpnflagoff_wholesale= "false",
        charging_cost_price = "IntradayPrice",
        charging_cost_assumption = "PreviousEFABlock"
    ) -> pd.DataFrame:
        """Gets GB index data for the given parameters.

        Args:
            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Set equal to the start date to return data for a given day.

            index_id `str`: The index ID denoting which index to get data for. Index IDs can be found on the GB Index page on Enact, by clicking the information icon next to an index.

            normalisation `str` (optional): The normalisation to apply. "Pound", "PoundPerMw", "PoundPerMwh", "PoundPerThroughput" or "PoundPerKwPerYear" (default).

            granularity `str` (optional): The granularity of the data. "Day", "Week" (default), "Month" or "Year".

            show_profit `str` (optional): Set to "true" to show profit or "false" (default) to show revenue.

            gas_price_assumption `str` (optional): The price the gas costs are calculated against. "DayAheadForward" (default), "DayAheadSpot", "WithinDaySpot", "CheapestPrice" or None.

            market_price_assumption `str` (optional): The price the wholesale revenues are calculated against. "EpexDayAheadPrice", "WeightedAverageDayAheadPrice" (default), "NordpoolDayAheadPrice", "IntradayPrice" or "BestPrice".

            account_for_availability_in_normalisation `str` (optional): Set to "true" to account for availability in normalisation or "false" (default).

            include_wholesale_split `str` (optional): Set to "true" to show wholesale import and export profit split or "false" (default).

            bm_split_out_option `str` (optional): "BidOfferSplit" or "SystemEnergySplit" or None (default).

            ancillary_revenue_type `str` (optional): "FrequencyAndReserve", "ByProduct" (default) or "ByDirection".

            group_dx `str` (optional): Set to "true" to group all Dx services of the same direction together or "false" (default).

            include_capacity_market `str` (optional): Set to "true" (default) to include capacity market profits or "false".

            include_non_delivery_charges `str` (optional): Set to "true" (default) to include non-delivery charges or "false".

            include_imbalance `str` (optional): Set to "false" (default) to exclude imbalance payments for non-BM and secondary BMUs.

            include_estimated_charging_cost `str` (optional): Set to "false" (default) to exclude estimated charging/discharging costs for non-BM and secondary BMUs.

            include_fpnflagoff_wholesale `str` (optional): Set to "false" (default) to exclude estimated wholesale revenue for BM (No FPN) assets.

            charging_cost_price `str` (optional): The price assumption using for the estimated charging/discharging costs of non-BM and secondary BMUs. Options are: 'WeightedAverageDayAheadPrice', 'EpexDayAheadPrice', 'NordpoolDayAheadPrice', 'IntradayPrice' (default) or 'SystemPrice'.

            charging_cost_assumption `str` (optional): The charging cost assumption used for the estimated charging/discharging costs for non-BM and secondary BMUs. Options are: 'PreviousEFABlock' (default), 'OptimalPricePrev12Hours' and 'CurrentSp'."""
        request_body = index_service.generate_gb_request(
            date_from,
            date_to,
            index_id,
            normalisation,
            granularity,
            show_profit,
            gas_price_assumption,
            market_price_assumption,
            account_for_availability_in_normalisation,
            include_wholesale_split,
            bm_split_out_option,
            ancillary_revenue_type,
            group_dx,
            include_capacity_market,
            include_non_delivery_charges,
            include_imbalance,
            include_estimated_charging_cost,
            include_fpnflagoff_wholesale,
            charging_cost_price,
            charging_cost_assumption
        )
        response = self._post_request(self.endpoints.GB_INDEX_DATA, request_body)
        return index_service.process_index_data_response(response)

    async def get_gb_index_data_async(
        self,
        date_from: datetime,
        date_to: datetime,
        index_id: str,
        normalisation="PoundPerKwPerYear",
        granularity="Week",
        show_profit = "false",
        gas_price_assumption = "DayAheadForward",
        market_price_assumption = "WeightedAverageDayAheadPrice",
        account_for_availability_in_normalisation = "false",
        include_wholesale_split = "false",
        bm_split_out_option = None,
        ancillary_revenue_type = "ByProduct",
        group_dx = "false",
        include_capacity_market = "true",
        include_non_delivery_charges = "true",
        include_imbalance= "false",
        include_estimated_charging_cost= "false",
        include_fpnflagoff_wholesale= "false",
        charging_cost_price = None,
        charging_cost_assumption = None
    ) -> pd.DataFrame:
        """An asynchronous version of `get_gb_index_data`."""
        request_body = index_service.generate_gb_request(
            date_from,
            date_to,
            index_id,
            normalisation,
            granularity,
            show_profit,
            gas_price_assumption,
            market_price_assumption,
            account_for_availability_in_normalisation,
            include_wholesale_split,
            bm_split_out_option,
            ancillary_revenue_type,
            group_dx,
            include_capacity_market,
            include_non_delivery_charges,
            include_imbalance,
            include_estimated_charging_cost,
            include_fpnflagoff_wholesale,
            charging_cost_price,
            charging_cost_assumption
        )
        response = await self._post_request_async(self.endpoints.GB_INDEX_DATA, request_body)
        return index_service.process_index_data_response(response)


    def get_default_german_indices(self) -> pd.DataFrame:
        """ Get the defining information of the default german indices, including the GUID that allows querying of that indices data via `get_german_index_data` """

        response = self._get_request(self.endpoints.EUROPE_INDEX_DEFAULT_INDICES)
        return index_service.process_index_info_response(response)

    async def get_default_german_indices_async(self) -> pd.DataFrame:
        """An asynchronous version of `get_default_german_indices`."""

        response = await self._get_request_async(self.endpoints.EUROPE_INDEX_DEFAULT_INDICES)
        return index_service.process_index_info_response(response)

    def get_europe_index_information(
        self,
        index_id: str,
        ) -> pd.DataFrame:
        """ Get the defining information of the index with the given ID.
         Args:
            index_id `str`: The index ID denoting which index to get data for. Index IDs can be found on the European Index page on Enact, by clicking the ID icon next to an index."""
        request_body = index_service.generate_index_info_request(
            index_id,
        )
        response = self._post_request(self.endpoints.EUROPE_INDEX_INFORMATION, request_body)
        return index_service.process_index_info_response(response)

    async def get_europe_index_information_async(
        self,
        index_id: str,
        ) -> pd.DataFrame:
        """An asynchronous version of `get_europe_index_information`."""
        request_body = index_service.generate_index_info_request(
            index_id,
        )
        response = await self._post_request_async(self.endpoints.EUROPE_INDEX_INFORMATION, request_body)
        return index_service.process_index_info_response(response)

    def get_german_index_data(
        self,
        date_from: datetime,
        date_to: datetime,
        index_id: str,
        normalisation="EuroPerKwPerYear",
        granularity="Week",
    ) -> pd.DataFrame:
        """Gets german index data for a given date range and index ID.

        Args:
            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Set equal to the start date to return data for a given day.

            index_id `str`: The index ID denoting which index to get data for. Index ID's of the default german indices can be found via the #### method.

            normalisation `str` (optional): The normalisation to apply. "Euro", "EuroPerMw", "EuroPerMwh" or "EuroPerKwPerYear" (default).

            granularity `str` (optional): The granularity of the data. "Day", "Week" (default), "Month" or "Year". """
        request_body = index_service.generate_request(
            date_from,
            date_to,
            index_id,
            normalisation,
            granularity,
        )
        response = self._post_request(self.endpoints.EUROPE_INDEX_DATA, request_body)
        return index_service.process_index_data_response(response)

    async def get_german_index_data_async(
        self,
        date_from: datetime,
        date_to: datetime,
        index_id: str,
        normalisation="PoundPerKwPerYear",
        granularity="Week",
    ) -> pd.DataFrame:
        """An asynchronous version of `get_german_index_data`."""
        request_body = index_service.generate_request(
            date_from,
            date_to,
            index_id,
            normalisation,
            granularity,
        )
        response = await self._post_request_async(self.endpoints.EUROPE_INDEX_DATA, request_body)
        return index_service.process_index_data_response(response)

    def get_contract_evolution(
        self,
        instrument,
        contract,
        contract_period = None,
        date_from: datetime = None,
        date_to: datetime = None,
    ) -> pd.DataFrame:
        """Gets the evolution of a commodity contracts closing prices. This returns a dataframe of the closing bid, offer and mid prices.
           The date column is the date on which this contract was traded to give these prices.

        Args:
            instrument `str`: The commodity instrument, "Nbp", "Eua", "UkaFutures", "UkPeak" or "UkBaseload".
            contract `str`: The contract type, "Spot", "WithinDay", "DayAhead", "BalanceOfWeek", "Weekend", "WorkingDaysNextWeek", "Week", "BalanceOfMonth", "Month", "Quarter", "Season", "Annuals" or "CalenderYear".
.           contract_period `str`: The contract period. Certain contracts can have multiple periods traded on a single day, for example the NBP monthly contract may trade the next 3 following months given the day of trade.
                                   In this instance the specific contract period must be specified. Contracts of Month or lower granularity require this contract_period. See our docs to see the format required of the contract_period for the different contracts.

            date_from `datetime.datetime` (optional): A start date to filter the returned data, if not given all data up to the date_to (also optional) will be returned.

            date_to `datetime.datetime`: An end date to filter the returned data, if not given all data from the date_from (also optional) will be returned. """

        request_body = contract_evolution_service.generate_request(
            instrument,
            contract,
            contract_period,
            date_from,
            date_to,
        )
        response = self._post_request(self.endpoints.CONTRACT_EVOLUTION, request_body)
        return contract_evolution_service.process_contract_evolution_response(response)

    async def get_contract_evolution_async(
        self,
        instrument,
        contract,
        contract_period = None,
        date_from: datetime = None,
        date_to: datetime = None,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_contract_evolution`."""
        request_body = contract_evolution_service.generate_request(
            instrument,
            contract,
            contract_period,
            date_from,
            date_to,
        )
        response = await self._post_request_async(self.endpoints.CONTRACT_EVOLUTION, request_body)
        return contract_evolution_service.process_contract_evolution_response(response)

    def get_ancillary_contract_data(
        self,
        ancillary_contract_type: str,
        option_one: Union[str, int] | None = None,
        option_two: Union[int, str] | None = None,
        date_requested: datetime | None = None,
    ) -> pd.DataFrame:
        """Get data for a specified Ancillary contract type.

        Args:
            ancillary_contract_type `str`: The type of ancillary contract being requested;
                "DynamicContainmentEfa" (DC-L), "DynamicContainmentEfaHF" (DC-H), "DynamicModerationLF" (DM-L), "DynamicModerationHF" (DM-H),
                "DynamicRegulationLF" (DR-L), "DynamicRegulationHF" (DR-H), "Ffr" (FFR), "StorDayAhead" (STOR), "ManFr" (MFR), "SFfr" (SFFR).

            option_one `str` or `int`: Additional information dependent on ancillary contract type. Tender Round (e.g. "150") for "FFR",
                Year-Month-Day (e.g. "2022-11-3") for "STOR", Year (e.g. "2022") for "MFR", and Month-Year (e.g. "11-2022") otherwise.

            option_two `str` (optional): Additional information dependent on ancillary contract type. Not applicable for "FFR" and "STOR".
                Month (e.g. "November") for "MFR", and Day (e.g. "5") otherwise.

            Returns:
                Response: A pandas DataFrame containing ancillary contract data for the requested date range.
        """
        contract_type = ancillary_service.try_parse_ancillary_contract_group_enum(ancillary_contract_type)
        request_body = ancillary_service.generate_ancillary_request(
            contract_type, option_one, option_two, date_requested
        )
        response = self._post_request(self.endpoints.ANCILLARY, request_body)
        return ancillary_service.process_ancillary_response(response, contract_type)

    async def get_ancillary_contract_data_async(
        self,
        ancillary_contract_type: str,
        option_one: Union[str, int] | None = None,
        option_two: Union[int, str] | None = None,
        date_requested: datetime | None = None,
    ) -> pd.DataFrame:
        """An asynchronous version of `get_ancillary_contract_data`."""
        contract_type = ancillary_service.try_parse_ancillary_contract_group_enum(ancillary_contract_type)
        request_body = ancillary_service.generate_ancillary_request(
            contract_type, option_one, option_two, date_requested
        )
        response = await self._post_request_async(self.endpoints.ANCILLARY, request_body)
        return ancillary_service.process_ancillary_response(response, contract_type)

    def get_DCL_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DCL (Dynamic Containment Low) contracts for a provided day.

        Args:
            date_requested `datetime.datetime`: The date for which to retrieve DCL contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("DynamicContainmentEfa", None, date_requested.day, date_requested)

    async def get_DCL_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_DCL_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "DynamicContainmentEfa", None, date_requested.day, date_requested
        )

    def get_DCH_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DCH (Dynamic Containment High) contracts for a provided day.

        Args:
            date_requested `datetime.date`: The date for which to retrieve DCH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("DynamicContainmentEfaHF", None, date_requested.day, date_requested)

    async def get_DCH_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_DCH_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "DynamicContainmentEfaHF", None, date_requested.day, date_requested
        )

    def get_DML_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DML (Dynamic Moderation Low) contracts for a provided day.

        Args:
            date_requested `datetime.datetime`: The date for which to retrieve DML contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("DynamicModerationLF", None, date_requested.day, date_requested)

    async def get_DML_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_DML_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "DynamicModerationLF", None, date_requested.day, date_requested
        )

    def get_DMH_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DMH (Dynamic Moderation High) contracts for a provided day.

        Args:
            date_requested `datetime.datetime`: The date for which to retrieve DMH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("DynamicModerationHF", None, date_requested.day, date_requested)

    async def get_DMH_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_DMH_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "DynamicModerationHF", None, date_requested.day, date_requested
        )

    def get_DRL_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DRL (Dynamic Regulation Low) contracts for a provided day.

        Args:
            date_requested `datetime.date`: The date for which to retrieve DRL contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("DynamicRegulationLF", None, date_requested.day, date_requested)

    async def get_DRL_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_DRL_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "DynamicRegulationLF", None, date_requested.day, date_requested
        )

    def get_DRH_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DRH (Dynamic Regulation High) contracts for a provided day.

        Args:
            date `datetime.date`: The date for which to retrieve DRH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("DynamicRegulationHF", None, date_requested.day, date_requested)

    async def get_DRH_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_DRH_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "DynamicRegulationHF", None, date_requested.day, date_requested
        )

    def get_NBR_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns NBR (Negative Balancing Reserve) contracts for a provided day.

        Args:
            date `datetime.date`: The date for which to retrieve DRH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("NegativeBalancingReserve", None, date_requested.day, date_requested)

    async def get_NBR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_nbr_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "NegativeBalancingReserve", None, date_requested.day, date_requested
        )

    def get_PBR_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns PBR (Positive Balancing Reserve) contracts for a provided day.

        Args:
            date `datetime.date`: The date for which to retrieve DRH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("PositiveBalancingReserve", None, date_requested.day, date_requested)

    async def get_PBR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_PBR_contracts`."""
        return await self.get_ancillary_contract_data_async(
            "PositiveBalancingReserve", None, date_requested.day, date_requested
        )

    def get_FFR_contracts(self, tender_number: int) -> pd.DataFrame:
        """Returns FFR (Firm Frequency Response) tender results for a given tender round.

        Args:
            tender_number `int`: The tender number for the round that you wish to procure
        """
        return self.get_ancillary_contract_data("Ffr", tender_number)

    async def get_FFR_contracts_async(self, tender_number: int) -> pd.DataFrame:
        """An asynchronous version of `get_FFR_contracts`."""
        return await self.get_ancillary_contract_data_async("Ffr", tender_number)

    def get_STOR_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns STOR (Short Term Operating Reserve) results for a given date.

        Args:
            date_requested `datetime.date`: The date for which to retrieve STOR contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("StorDayAhead", date_requested=date_requested)

    async def get_STOR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_STOR_contracts`."""
        return await self.get_ancillary_contract_data_async("StorDayAhead", date_requested=date_requested)

    def get_SFFR_contracts(self, date_requested: datetime) -> pd.DataFrame:
        """Returns SFFR (Static Firm Frequency Response) results for a given date.

        Args:
            date_requested `datetime.date`: The date for which to retrieve SFFR contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """
        return self.get_ancillary_contract_data("SFfr", None, date_requested.day, date_requested)

    async def get_SFFR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_SFFR_contracts`."""
        return await self.get_ancillary_contract_data_async("SFfr", None, date_requested.day, date_requested)

    def get_MFR_contracts(self, month: int, year: int) -> pd.DataFrame:
        """Returns MFR tender results for a given month and year asynchronously.

        Args:
            month `int`: Corresponding month for the data requested
            year `int`: Corresponding year for the data requested
        """
        return self.get_ancillary_contract_data("ManFr", year, get_month_name(month))

    async def get_MFR_contracts_async(self, month: int, year: int) -> pd.DataFrame:
        """An asynchronous version of `get_MFR_contracts`."""
        return await self.get_ancillary_contract_data_async("ManFr", year, get_month_name(month))

    def get_news_table(self, table_id: str) -> pd.DataFrame:
        """Gets the specified news table.

        Args:
            table_id `str`: This is the News table you would like the data from;
            "BmStartupDetails" "Bsad" "CapacityChanges" "Traids" "Elexon" "LCP" "Entsoe"

        """
        request_body = news_table_service.generate_request(table_id)
        response = self._post_request(self.endpoints.NEWS_TABLE, request_body)
        return news_table_service.process_response(response)

    async def get_news_table_async(self, table_id: str) -> pd.DataFrame:
        """An asynchronous version of `get_news_table`."""
        request_body = news_table_service.generate_request(table_id)
        response = await self._post_request_async(self.endpoints.NEWS_TABLE, request_body)
        return news_table_service.process_response(response)

    def get_epex_trades_by_contract_id(self, contract_id: str) -> pd.DataFrame:
        """Gets executed EPEX trades for a given contract ID.

        Args:
            contract_id `int`: The ID associated with the EPEX contract you would like executed trades for.

        """
        request_body = epex_service.generate_contract_id_request(contract_id)
        response = self._post_request(self.endpoints.EPEX_TRADES_BY_CONTRACT_ID, request_body)
        return epex_service.process_trades_response(response)

    async def get_epex_trades_by_contract_id_async(self, contract_id: str) -> pd.DataFrame:
        """An asynchronous version of `get_epex_trades_by_contract_id`."""
        request_body = epex_service.generate_contract_id_request(contract_id)
        response = await self._post_request_async(self.endpoints.EPEX_TRADES_BY_CONTRACT_ID, request_body)
        return epex_service.process_trades_response(response)

    def get_epex_trades(self, type: str, date: datetime, period: int = None) -> pd.DataFrame:
        """Gets executed EPEX trades of a contract given the date, period and type.

        Args:
            type: The EPEX contract type; "HH", "1H", "2H", "4H", "3 Plus 4", "Overnight", "Peakload", "Baseload", or "Ext. Peak".

            date `datetime.datetime`: The date to request EPEX trades for.

            period `int` (optional): The period for which to retrieve the EPEX trades. If None and date input is of type datetime, the period is calculated (rounded down to the nearest half-hour).

        Raises:
            `TypeError`: If the period is not an integer or if no period is given and date is not of type datetime.

        """
        request_body = epex_service.generate_time_and_type_request(type, date, period)
        response = self._post_request(self.endpoints.EPEX_TRADES, request_body)
        return epex_service.process_trades_response(response)

    async def get_epex_trades_async(self, type: str, date: datetime, period: int = None) -> pd.DataFrame:
        """An asynchronous version of `get_epex_trades`."""
        request_body = epex_service.generate_time_and_type_request(type, date, period)
        response = await self._post_request_async(self.endpoints.EPEX_TRADES, request_body)
        return epex_service.process_trades_response(response)

    def get_epex_order_book(self, type: str, date: datetime, period: int = None) -> dict[str, pd.DataFrame]:
        """Gets the order book of a contract given a date, period and type.

        Args:
            type: The EPEX contract type; "HH", "1H", "2H", "4H", "3 Plus 4", "Overnight", "Peakload", "Baseload", or "Ext. Peak".

            date `datetime.datetime`: The date to request EPEX trades for.

            period `int` (optional): The period for which to retrieve the EPEX trades. If None and date input is of type datetime, the period is calculated (rounded down to the nearest half-hour).

        Raises:
            `TypeError`: If the period is not an integer or if no period is given and date is not of type datetime.

        """
        request_body = epex_service.generate_time_and_type_request(type, date, period)
        response = self._post_request(self.endpoints.EPEX_ORDER_BOOK, request_body)
        return epex_service.process_order_book_response(response)

    async def get_epex_order_book_async(self, type: str, date: datetime, period: int = None) -> dict[str, pd.DataFrame]:
        """An asynchronous version of `get_epex_order_book`."""
        request_body = epex_service.generate_time_and_type_request(type, date, period)
        response = await self._post_request_async(self.endpoints.EPEX_ORDER_BOOK, request_body)
        return epex_service.process_order_book_response(response)

    def get_epex_order_book_by_contract_id(self, contract_id: int) -> dict[str, pd.DataFrame]:
        """Gets the EPEX order book for a given contract ID.

        Args:
            contract_id `int`: The ID associated with the EPEX contract you would like the order book for.

        """
        request_body = epex_service.generate_contract_id_request(contract_id)
        response = self._post_request(self.endpoints.EPEX_ORDER_BOOK_BY_CONTRACT_ID, request_body)
        return epex_service.process_order_book_response(response)

    async def get_epex_order_book_by_contract_id_async(self, contract_id: int) -> dict[str, pd.DataFrame]:
        """An asynchronous version of `get_epex_order_book_by_contract_id`."""
        request_body = epex_service.generate_contract_id_request(contract_id)
        response = await self._post_request_async(self.endpoints.EPEX_ORDER_BOOK_BY_CONTRACT_ID, request_body)
        return epex_service.process_order_book_response(response)

    def get_epex_contracts(self, date: datetime) -> pd.DataFrame:
        """Gets EPEX contracts for a given day.

        Args:
            date `datetime.datetime`: The date you would like all contracts for.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.

        """
        request_body = epex_service.generate_contract_request(date)
        response = self._post_request(self.endpoints.EPEX_CONTRACTS, request_body, long_timeout=True)
        return epex_service.process_contract_response(response)

    async def get_epex_contracts_async(self, date: datetime) -> pd.DataFrame:
        """An asynchronous version of `get_epex_contracts`."""
        request_body = epex_service.generate_contract_request(date)
        response = await self._post_request_async(self.endpoints.EPEX_CONTRACTS, request_body)
        return epex_service.process_contract_response(response)

    def get_N2EX_buy_sell_curves(self, date: datetime) -> dict:
        """Gets N2EX buy and sell curves for a given day.

        Args:
            date `datetime.datetime`: The date you would like buy and sell curves for.

        """
        request_body = nordpool_service.generate_request(date)
        return self._post_request(self.endpoints.NORDPOOL_CURVES, request_body)

    async def get_N2EX_buy_sell_curves_async(self, date: datetime) -> dict:
        """An asynchronous version of `get_N2EX_buy_sell_curves`."""
        request_body = nordpool_service.generate_request(date)
        return await self._post_request_async(self.endpoints.NORDPOOL_CURVES, request_body)

    def get_day_ahead_data(
        self,
        fromDate: datetime,
        toDate: datetime | None = None,
        aggregate: bool = False,
        numberOfSimilarDays: int = 10,
        selectedEfaBlocks: int | None = None,
        seriesInput: list[str] = None,
    ) -> dict[int, pd.DataFrame]:
        """Find historical days with day ahead prices most similar to the current day.

        Args:
            from `datetime.datetime`: The start of the date range to compare against.

            to `datetime.datetime`: The end of the date range for days to compare against.

            aggregate `bool` (optional): If set to true, the EFA blocks are considered as a single time range.

            numberOfSimilarDays `int` (optional): The number of the most similar days to include in the response.

            selectedEfaBlocks `int` (optional): The EFA blocks to find similar days for.

            seriesInput `list[str]` (optional): The series to find days with similar values to. Accepted values: "ResidualLoad", "Tsdf", "WindForecast", "SolarForecast"
            "DynamicContainmentEfa", "DynamicContainmentEfaHF", "DynamicContainmentEfaLF", "DynamicRegulationHF", "DynamicRegulationLF", "DynamicModerationLF",
            "DynamicModerationHF", "PositiveBalancingReserve", "NegativeBalancingReserve", "SFfr". If none specified, all are used in the calculation.
        Raises:
            `TypeError`: If the input dates are not of type date or datetime.

        """
        request_body = day_ahead_service.generate_request(
            fromDate, toDate, aggregate, numberOfSimilarDays, selectedEfaBlocks, seriesInput
        )
        response = self._post_request(self.endpoints.DAY_AHEAD, request_body)
        return day_ahead_service.process_response(response)

    async def get_day_ahead_data_async(
        self,
        fromDate: datetime,
        toDate: datetime | None = None,
        aggregate: bool = False,
        numberOfSimilarDays: int = 10,
        selectedEfaBlocks: int | None = None,
        seriesInput: list[str] = None,
    ) -> dict[int, pd.DataFrame]:
        """An asynchronous version of `get_day_ahead_data`."""
        request_body = day_ahead_service.generate_request(
            fromDate, toDate, aggregate, numberOfSimilarDays, selectedEfaBlocks, seriesInput
        )
        response = await self._post_request_async(self.endpoints.DAY_AHEAD, request_body)
        return day_ahead_service.process_response(response)
