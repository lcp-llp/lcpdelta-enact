import calendar
from datetime import date, datetime
import pandas as pd

import handlers.hof_handler
from .helpers import convert_response_to_df, convert_dict_to_df, convert_embedded_list_to_df
from lcp_delta.global_helpers import is_list_of_strings, get_period, convert_datetime_to_iso, convert_datetimes_to_iso
from typing import Union
import handlers
from enums import AncillaryContractGroup

from ..common import APIHelperBase, constants


class APIHelper(APIHelperBase):
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
    ) -> pd.DataFrame | dict:
        """Makes a request to the series endpoints asynchronously.

        Returns:
             Response: A dictionary or pandas DataFrame containing the series data.
        """
        request_body = handlers.series_handler.generate_series_data_request(
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
        return handlers.series_handler.series_data_post_process(response)

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
    ) -> pd.DataFrame | dict:
        """Makes a request to the series endpoints.

        Returns:
             Response: A dictionary or pandas DataFrame containing the series data.
        """
        request_body = handlers.series_handler.generate_series_data_request(
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
        return handlers.series_handler.series_data_post_process(response)

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
        """Gets series data from Enact asynchronously.

        Args:
            series_id `str`: The Enact series ID.

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_id `list[str]`: The Enact option ID, if an option is applicable.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Data_V2"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        return await self._make_series_request_async(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            option_id,
            half_hourly_average,
            endpoint,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

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

            option_id `list[str]`: The Enact option ID, if an option is applicable.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Data_V2"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        return self._make_series_request(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            option_id,
            half_hourly_average,
            endpoint,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    async def get_series_info_async(self, series_id: str, country_id: str | None = None) -> dict:
        """Gets information about a specific Enact series asynchronously.

        Args:
            series_id `str`: The series ID.
            country_id `str` (optional): The country ID (defaults to None). If not provided, data will be returned for the first country available for this series.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            An object containing information about the series. This includes series name, countries with data for that series, options related to the series, whether or not the series has historical data, and whether
            or not the series has historical forecasts.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Info"
        request_body = handlers.series_handler.generate_series_info_request(series_id, country_id)
        return await self._post_request_async(endpoint, request_body)

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
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Info"
        request_body = handlers.series_handler.generate_series_info_request(series_id, country_id)
        return self._post_request(endpoint, request_body)

    async def get_series_by_fuel_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: str,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Gets plant series data for a given fuel type asynchronously.

        Args:
            series_id `str`: The Enact series ID.

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_id `list[str]`: The Enact option ID, if an option is applicable.

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Fuel"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        fuel_type = [option_id]
        return await self._make_series_request_async(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            fuel_type,
            half_hourly_average,
            endpoint,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    def get_series_by_fuel(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: str,
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

            option_id `list[str]`: The fuel option for the request, e.g. "Coal".

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Fuel"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        fuel_type = [option_id]
        return self._make_series_request(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            fuel_type,
            half_hourly_average,
            endpoint,
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
        option_id: str,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Get plant series data for a given zone asynchronously.

        Args:
            series_id `str`: The Enact series ID (must be a plant series).

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_id `str`: The fuel option for the request, e.g. "Z1".

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Zone"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        zone = [option_id]
        return await self._make_series_request_async(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            zone,
            half_hourly_average,
            endpoint,
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
        option_id: str,
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

            option_id `str`: The fuel option for the request, e.g. "Z1".

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Zone"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        zone = [option_id]
        return self._make_series_request(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            zone,
            half_hourly_average,
            endpoint,
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
        option_id: str,
        half_hourly_average: bool = False,
        request_time_zone_id: str | None = None,
        time_zone_id: str | None = None,
        parse_datetimes: bool = False,
    ) -> pd.DataFrame:
        """Get plant series data for a given owner asynchronously.

        Args:
            series_id `str`: The Enact series ID (must be a plant series).

            date_from `datetime.datetime`: The start date.

            date_to `datetime.datetime`: The end date. Can be set equal to start date to return one days' data.

            option_id `str`: The owner option for the request, e.g. "Adela Energy".

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Owner"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        owner = [option_id]
        return await self._make_series_request_async(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            owner,
            half_hourly_average,
            endpoint,
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
        option_id: str,
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

            option_id `str`: The owner option for the request, e.g. "Adela Energy".

            country_id `str` (optional): The country ID for filtering the data. Defaults to "Gb".

            half_hourly_average `bool` (optional): Retrieve half-hourly average data. Defaults to False.

            request_time_zone_id `str` (optional): Time zone ID of the requested time range. Defaults to GMT/BST.

            time_zone_id `str` (optional): Time zone ID of the data to be returned. Defaults to UTC.

            parse_datetimes `bool` (optional): Parse returned DataFrame index to DateTime (UTC). Defaults to False.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: An object containing the series data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/Owner"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        owner = [option_id]
        return self._make_series_request(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            owner,
            half_hourly_average,
            endpoint,
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
        """Get series data for a specific series with multiple options available asynchronously.

        Args:
            series_id `str`: The Enact series ID (must be a plant series).

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
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/multiOption"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        return await self._make_series_request_async(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            option_id,
            half_hourly_average,
            endpoint,
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
        """Get series data for a specific series with multiple options available.

        Args:
            series_id `str`: The Enact series ID (must be a plant series).

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
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Series/multiOption"
        date_from_str, date_to_str = convert_datetimes_to_iso(date_from, date_to)
        return self._make_series_request(
            series_id,
            date_from_str,
            date_to_str,
            country_id,
            option_id,
            half_hourly_average,
            endpoint,
            request_time_zone_id,
            time_zone_id,
            parse_datetimes,
        )

    async def get_plant_details_by_id_async(self, plant_id: str) -> dict:
        """Get details of a plant based on the plant ID asynchronously.

        Args:
            plant_id `str`: The Enact plant ID.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantInfo"
        request_body = {"PlantId": plant_id}
        return await self._post_request_async(endpoint, request_body)

    def get_plant_details_by_id(self, plant_id: str) -> dict:
        """Get details of a plant based on the plant ID.

        Args:
            plant_id `str`: The Enact plant ID.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantInfo"
        request_body = {"PlantId": plant_id}
        return self._post_request(endpoint, request_body)

    async def get_plants_by_fuel_and_country_async(self, fuel_id: str, country_id: str) -> list[str]:
        """Get a list of plants for a given fuel and country asynchronously.

        Args:
            fuel_id `str`: The fuel ID.
            country_id `str` (optional): The country ID. Defaults to "Gb".

        Returns:
            Response: The response object containing the plant data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantList"
        request_body = {"Country": country_id, "Fuel": fuel_id}
        response = await self._post_request_async(endpoint, request_body)
        return response["data"]

    def get_plants_by_fuel_and_country(self, fuel_id: str, country_id: str) -> list[str]:
        """Get a list of plants for a given fuel and country.

        Args:
            fuel_id `str`: The fuel ID.
            country_id `str` (optional): The country ID. Defaults to "Gb".

        Returns:
            Response: The response object containing the plant data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Plant/Data/PlantList"
        request_body = {"Country": country_id, "Fuel": fuel_id}
        response = self._post_request(endpoint, request_body)
        return response["data"]

    async def get_history_of_forecast_for_given_date_async(
        self, series_id: str, date: datetime, country_id: str, option_id: str | None = None
    ) -> pd.DataFrame:
        """Gets the history (all iterations) of a series forecast for a given date asynchronously.

        Args:
            series_id `str`: The Enact series ID.

            date `datetime.date`: The date to request forecasts for.

            country_id `str` (optional): This Enact country ID. Defaults to "Gb".

            option_id `list[str]` (optional): The Enact option ID, if an option is applicable. Defaults to None.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A pandas DataFrame holding all data for the requested series on the requested date.
                    The first row will provide all the dates we have a forecast iteration for.
                    All other rows correspond to the data-points at each value of the first array.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/Data_V2"
        request_body = handlers.hof_handler.generate_hof_request_single_date(series_id, date, country_id, option_id)
        response = await self._post_request_async(endpoint, request_body)
        return convert_response_to_df(response, nested_key="data")

    def get_history_of_forecast_for_given_date(
        self, series_id: str, date: datetime, country_id: str, option_id: str | None = None
    ) -> pd.DataFrame:
        """Gets the history (all iterations) of a series forecast for a given date.

        Args:
            series_id `str`: The Enact series ID.

            date `datetime.date`: The date to request forecasts for.

            country_id `str` (optional): This Enact country ID. Defaults to "Gb".

            option_id `list[str]` (optional): The Enact option ID, if an option is applicable. Defaults to None.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A pandas DataFrame holding all data for the requested series on the requested date.
                    The first row will provide all the dates we have a forecast iteration for.
                    All other rows correspond to the data-points at each value of the first array.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/Data_V2"
        request_body = handlers.hof_handler.generate_hof_request_single_date(series_id, date, country_id, option_id)
        response = self._post_request(endpoint, request_body)
        return convert_response_to_df(response, nested_key="data")

    async def get_history_of_forecast_for_date_range_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        option_id: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Gets the history of a forecast for a given date asynchronously.

        Args:
            series_id `str`: The Enact series ID.

            date_from `datetime.datetime`: The start date to request forecasts for.

            date_to `datetime.datetime`: The end date to request forecasts for.

            country_id `str` (optional): This Enact country ID. Defaults to "Gb".

            option_id `list[str]` (optional): The Enact option IDs, if an options are applicable. Defaults to None.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A dictionary of strings and pandas DataFrame holding all data for the requested series on the requested date.
                    The first row will provide all the dates we have a forecast iteration for.
                    All other rows correspond to the data-points at each value of the first array.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/Data_V2"
        response_body = handlers.hof_handler.generate_hof_request_date_range(
            series_id, date_from, date_to, country_id, option_id
        )
        response = await self._post_request_async(endpoint, response_body)
        return handlers.hof_handler.date_range_post_process(response)

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

            option_id `list[str]` (optional): The Enact option IDs, if an options are applicable. Defaults to None.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A dictionary of strings and pandas DataFrames holding all data for the requested series on the requested date.
                    The first row will provide all the dates we have a forecast iteration for.
                    All other rows correspond to the data-points at each value of the first array.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/Data_V2"
        response_body = handlers.hof_handler.generate_hof_request_date_range(
            series_id, date_from, date_to, country_id, option_id
        )
        response = self._post_request(endpoint, response_body)
        return handlers.hof_handler.date_range_post_process(response)

    async def get_latest_forecast_generated_at_given_time_async(
        self,
        series_id: str,
        date_from: datetime,
        date_to: datetime,
        country_id: str,
        forecast_as_of: datetime,
        option_id: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Gets the latest forecast generated prior to the given 'forecast_as_of' datetime asynchronously.

        Args:
            series_id `str`: The Enact series ID.

            date_from `datetime.datetime`: The start date to request forecasts for.

            date_to `datetime.datetime`: The end date to request forecasts for.

            country_id `str` (optional): This Enact country ID. Defaults to "Gb".

            forecast_as_of `datetime.datetime`: The date you want the latest forecast generated for.

            option_id `list[str]` (optional): The Enact option IDs, if an options are applicable. Defaults to None.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A dictionary of string and pandas DataFrames, holding the latest forecast generated to the given 'forecast_as_of' datetime for the range of dates requested.
            The keys are the datetime strings of each of these dates. The first row of each DataFrame will provide the date we have a forecast iteration for, which will be the latest generated
            forecast before the given 'forecast_as_of' datetime. All other rows correspond to the data-points at each value of the first array.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/get_latest_forecast"
        request_body = handlers.hof_handler.generate_hof_request_latest_forecast(
            series_id, date_from, date_to, country_id, forecast_as_of, option_id
        )
        response = await self._post_request_async(endpoint, request_body)
        return handlers.hof_handler.latest_forecast_post_process(response)

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

            option_id `list[str]` (optional): The Enact option IDs, if an options are applicable. Defaults to None.

        Note that series, option and country IDs for Enact can be found at https://enact.lcp.energy/externalinstructions.

        Returns:
            Response: A dictionary of string and pandas DataFrames, holding the latest forecast generated to the given 'forecast_as_of' datetime for the range of dates requested.
            The keys are the datetime strings of each of these dates. The first row of each DataFrame will provide the date we have a forecast iteration for, which will be the latest generated
            forecast before the given 'forecast_as_of' datetime. All other rows correspond to the data-points at each value of the first array.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/HistoryOfForecast/get_latest_forecast"
        request_body = handlers.hof_handler.generate_hof_request_latest_forecast(
            series_id, date_from, date_to, country_id, forecast_as_of, option_id
        )
        response = self._post_request(endpoint, request_body)
        return handlers.hof_handler.latest_forecast_post_process(response)

    # BOA:
    async def get_bm_data_by_period_async(
        self, date: datetime, period: int = None, include_accepted_times: bool = False
    ) -> pd.DataFrame:
        """Get BM (Balancing Mechanism) data for a specific date and period.

        This method retrieves the BM (Balancing Mechanism) data for a specific date and period.
        The date and period can either be specified by a single datetime or by passing in an optional period input with a date.
        If specified by a single datetime, the closest period when rounding down to the nearest half an hour will be used.
        If specified by the period input, the date should be in the correct format, and the period should be an integer.

        Args:
            date: The date that you would like the BOD data for.
            period (optional): The period for which to retrieve the BM data. If None and date input is of type datetime, the period is calculated.
            include_accepted_times: Choose whether object include BOA accepted times or not

        Returns:
            Response: The response object containing the BM data.

        Raises:
            `TypeError`: If the period is not an integer or if no period is given and date is not of type datetime.
        """

        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/BOA/Data"

        period = get_period(date, period)

        date = convert_datetime_to_iso(date)

        request_details = {"Date": date, "Period": period}

        if include_accepted_times is not False:
            request_details["includeAcceptedTimes"] = "True"

        response = await self._post_request_async(endpoint, request_details)
        output: dict[str, pd.DataFrame] = {}
        df_columns = ["acceptedBids", "acceptedOffers", "tableOffers", "tableBids"]

        for key_str in df_columns:
            if key_str in response["data"]:
                output[key_str] = convert_response_to_df(response, nested_key=key_str)
        return output

    async def get_bm_data_by_search_async(
        self,
        date: datetime,
        option: str = "all",
        search_string: str | None = None,
        include_accepted_times: bool = False,
    ) -> pd.DataFrame:
        """Get BM data based for a specific date and search criteria.

        Args:
            date `datetime.datetime`: The date for which to retrieve the BM data.
            option `str`: This allows you to select whether you want to search for BOA data for plants, fuels or just return everything. Can be set to "plant", "fuel", "all"
            search_string `str`: The search string to match against the BM data. If Option is "plant", this allows you to search for all BOA actions from plants with BMU ID containing "CARR" (e.g. all Carrington units).
                                If option is "fuel", this allows you to search for all BOA actions from plants with fuel type "Coal". If Option is "all", this must not be sent to work.
            include_accepted_times `bool`: Determine whether the returned object includes a column for accepted times in the response object
        Returns:
            Response: The response object containing the BM data.
        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/BOA/Data"

        date = convert_datetime_to_iso(date)

        request_details = {"Date": date, "Option": option, "SearchString": search_string}

        if include_accepted_times is not False:
            request_details["includeAcceptedTimes"] = "True"

        response = await self._post_request_async(endpoint, request_details)
        return pd.DataFrame(response["data"][1:], columns=response["data"][0])

    # Leaderboard:
    async def get_leaderboard_data_async(
        self,
        date_from: datetime,
        date_to: datetime,
        type="Plant",
        revenue_metric="PoundPerMwPerH",
        market_price_assumption="WeightedAverageDayAheadPrice",
        gas_price_assumption="DayAheadForward",
        include_capacity_market_revenues=False,
    ) -> pd.DataFrame:
        """Get leaderboard data for a specific date range.

        Args:
            date_from `datetime.datetime`: The start date of the leaderboard data.

            date_to `datetime.datetime`: The end date of the leaderboard data.
                                     If a single day is wanted, then this will be the same as the From value.

            type `str`: The type of leaderboard to be requested.
                                     Possible options are: "Plant", "Owner" or "Battery".

            revenue_metric `str` (optional): This is the unit which revenues will be measured in for the leaderboard.
                                            Possible options are: Pound or PoundPerMwPerH.
                                            If not included the default is PoundPerMwPerH.

            market_price_assumption `str` (optional): This is the price assumption for wholesale revenues on the leaderboard.
                                                     Possible options are: WeightedAverageDayAheadPrice, EpexDayAheadPrice, NordpoolDayAheadPrice, IntradayPrice or BestPrice.
                                                     Defaults to WeightedAverageDayAheadPrice.

            gas_price_assumption `str` (optional): The gas price assumption to filter the leaderboard data.
                                                  Possible options are: DayAheadForward, DayAheadSpot, WithinDaySpot or CheapestPrice.
                                                  If not included the default is DayAheadForward.

            include_capacity_market_revenues `bool` (optional): Changes whether or not the capacity market revenue column is shown.
                                                    If set to false, the capacity market revenues will not be included in the net revenues. Defaults to False.
        """

        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Leaderboard/Data"

        date_from, date_to = convert_datetimes_to_iso(date_from, date_to)

        request_details = {
            "From": date_from,
            "To": date_to,
            "Type": type,
            "RevenueMetric": revenue_metric,
            "MarketPriceAssumption": market_price_assumption,
            "GasPriceAssumption": gas_price_assumption,
            "IncludeCmRevenues": include_capacity_market_revenues,
        }

        response = await self._post_request_async(endpoint, request_details)
        index = "Plant - Owner" if type == "Owner" else "Plant - ID"
        return convert_embedded_list_to_df(response, index)

    # Ancillary Contracts:
    async def get_ancillary_contract_data_async(
        self,
        ancillary_contract_type: str,
        option_one: Union[str, int] | None = None,
        option_two: Union[int, str] | None = None,
        date_requested: datetime | None = None,
        ancillary_contract_group: AncillaryContractGroup | None = None,
    ) -> pd.DataFrame:
        """Get data for a specified Ancillary contract type.

        Args:
            ancillary_contract_type `str`: The type of ancillary contract being requested.
                                             The options are "DynamicContainmentEfa" (for DC-L), "DynamicContainmentEfaHF" (for DC-H), "DynamicModerationLF" (for DM-L), "DynamicModerationHF" (for DM-H),
                                               "DynamicRegulationLF" (for DR-L), "DynamicRegulationHF" (for DR-H), "Ffr" (for FFR), "StorDayAhead" (for STOR), "ManFr" (for MFR), "SFfr" (for SFFR).

            option_one `str` or `int`: Additional information dependent on ancillary contract type. Tender Round (e.g. "150") for "FFR", Year-Month-Day (e.g. "2022-11-3") for "STOR", Year (e.g. "2022") for
            "MFR", and Month-Year (e.g. "11-2022") otherwise.

            option_two `str` (optional): Additional information dependent on ancillary contract type. Not applicable for "FFR" and "STOR", Month (e.g. "November") for "MFR", and Day (e.g. "5") otherwise.

            Returns:
                Response: This holds all data for the ancillary contract type on the requested date/date range.
        """
        if date_requested:
            if not isinstance(date_requested, date | datetime):
                raise TypeError("Requested date must be a date or datetime")
            if (
                ancillary_contract_group == AncillaryContractGroup.Dynamic
                or ancillary_contract_group == AncillaryContractGroup.SFfr
            ):
                option_one = "-".join([str(date_requested.month), str(date_requested.year)])
            if ancillary_contract_group == AncillaryContractGroup.StorDayAhead:
                option_one = "-".join([str(date_requested.year), str(date_requested.month), str(date_requested.day)])

        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Ancillary/Data"

        request_details = {
            "AncillaryContractType": ancillary_contract_type,
            "OptionOne": option_one,
        }

        if option_two is not None:
            request_details["OptionTwo"] = option_two

        response = await self._post_request_async(endpoint, request_details)

        if "data" not in response or not response["data"]:
            return pd.DataFrame()
        first_item = response["data"][0]
        if ancillary_contract_group == AncillaryContractGroup.SFfr:
            return pd.DataFrame(first_item["plants"])
        if ancillary_contract_group == AncillaryContractGroup.ManFr:
            for entry in first_item["plants"]:
                entry.update(entry.pop("data"))
            df = pd.DataFrame(first_item["plants"])
            if not df.empty:
                unit_column_name = df.columns[0]
                df.set_index(unit_column_name, inplace=True)
            return df
        if ancillary_contract_group == AncillaryContractGroup.StorDayAhead:
            return pd.DataFrame(first_item["plants"])
        if ancillary_contract_group == AncillaryContractGroup.Ffr:
            df = pd.DataFrame(first_item["plants"])
            df.set_index("tenderNumber", inplace=True)
            return df
        if ancillary_contract_group == AncillaryContractGroup.Dynamic:
            return self._convert_api_response_for_dynamic_and_balancing_reserve_products(response)
        return response

    async def get_DCL_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DCL (Dynamic Containment Low) contracts for a provided day

        Args:
            date_requested `datetime.datetime`: The date for which to retrieve DCL contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "DynamicContainmentEfa",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    async def get_DCH_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DCH (Dynamic Containment High) contracts for a provided day

        Args:
            date_requested `datetime.date`: The date for which to retrieve DCH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "DynamicContainmentEfaHF",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    async def get_DML_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DML (Dynamic Moderation Low) contracts for a provided day

        Args:
            date_requested `datetime.datetime`: The date for which to retrieve DML contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "DynamicModerationLF",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    async def get_DMH_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DMH (Dynamic Moderation High) contracts for a provided day

        Args:
            date_requested `datetime.datetime`: The date for which to retrieve DMH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "DynamicModerationHF",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    async def get_DRL_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DRL (Dynamic Regulation Low) contracts for a provided day

        Args:
            date_requested `datetime.date`: The date for which to retrieve DRL contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "DynamicRegulationLF",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    async def get_DRH_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns DRH (Dynamic Regulation High) contracts for a provided day

        Args:
            date `datetime.date`: The date for which to retrieve DRH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "DynamicRegulationHF",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    async def get_NBR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns NBR (Negative Balancing Reserve) contracts for a provided day

        Args:
            date `datetime.date`: The date for which to retrieve DRH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "NegativeBalancingReserve",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    async def get_PBR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns PBR (Positive Balancing Reserve) contracts for a provided day

        Args:
            date `datetime.date`: The date for which to retrieve DRH contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "PositiveBalancingReserve",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.Dynamic,
        )

    def _convert_api_response_for_dynamic_and_balancing_reserve_products(self, response: dict) -> pd.DataFrame:
        df = pd.DataFrame(response["data"][0]["plants"])
        if not df.empty:
            df.set_index("orderId", inplace=True)
        return df

    async def get_FFR_contracts_async(self, tender_number: int) -> pd.DataFrame:
        """Returns FFR (Firm Frequency Response) tender results for a given tender round

        Args:
            tender_number `int`: The tender number for the round that you wish to procure
        """
        return await self.get_ancillary_contract_data_async(
            "Ffr", tender_number, ancillary_contract_group=AncillaryContractGroup.Ffr
        )

    async def get_STOR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns STOR (Short Term Operating Reserve) results for a given date

        Args:
            date_requested `datetime.date`: The date for which to retrieve STOR contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "StorDayAhead", date_requested=date_requested, ancillary_contract_group=AncillaryContractGroup.StorDayAhead
        )

    async def get_SFFR_contracts_async(self, date_requested: datetime) -> pd.DataFrame:
        """Returns SFFR (Static Firm Frequency Response) results for a given date

        Args:
            date_requested `datetime.date`: The date for which to retrieve SFFR contracts.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.
        """

        return await self.get_ancillary_contract_data_async(
            "SFfr",
            option_two=date_requested.day,
            date_requested=date_requested,
            ancillary_contract_group=AncillaryContractGroup.SFfr,
        )

    async def get_MFR_contracts_async(self, month: int, year: int) -> pd.DataFrame:
        """Returns MFR tender results for a given month and year

        Args:
            month `int`: Corresponding month for the data requested
            year `int`: Corresponding year for the data requested
        """
        if not 0 < month <= 12:
            raise ValueError("Month must be an integer less than 12")
        month_name = calendar.month_name[month]

        return await self.get_ancillary_contract_data_async(
            "ManFr", year, month_name, ancillary_contract_group=AncillaryContractGroup.ManFr
        )

    # News table
    async def get_news_table_async(self, table_id: str) -> pd.DataFrame:
        """Will return the selected news table you would like data from.

        Args:
            table_id `str`: This is the News table you would like the data from. The options include:

                            Table Header                                            Table ID:
                            BM Warming Instructions	                                BmStartupDetails
                            Forward BSAD Trades	                                    Bsad
                            Additional MW changes (additional GT / SEL reduction	CapacityChanges
                            Triads	                                                Traids
                            Elexon System Warnings	                                Elexon
                            LCP Alerts	                                            LCP
                            Entsoe News	                                            Entsoe

        """
        if table_id.lower() == "lcp":
            table_id = "Lcp"

        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/Newstable/Data"
        request_details = {
            "TableId": table_id,
        }

        response = await self._post_request_async(endpoint, request_details)

        return convert_embedded_list_to_df(response)

    # EPEX:
    async def get_epex_trades_by_contract_id_async(self, contract_id: str) -> pd.DataFrame:
        """Get executed EPEX trades of a contract, given the Contract ID

        Args:
            contract_id `int`: The ID associated with the EPEX contract you would like executed trades for.

        """
        endpoint = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/TradesFromContractId"

        request_details = {
            "ContractId": contract_id,
        }

        response = await self._post_request_async(endpoint, request_details)
        return convert_response_to_df(response, index_on=-1)

    async def get_epex_trades_async(self, type: str, date: datetime, period: int = None) -> pd.DataFrame:
        """Get executed EPEX trades of a contract, given the date, period and type

        The date and period can either be specified by a datetime or by passing in an optional period input with a date.
        If specified by a datetime, the closest period when rounding down to the nearest half an hour will be used.
        If specified by the period input, and the period should be an integer.

        Args:
            type: The type associated with the EPEX contract you would like executed trades for. The options are "HH", "1H", "2H", "4H", "3 Plus 4", "Overnight", "Peakload", "Baseload", "Ext. Peak".

            date: The date associated with the EPEX contract you would like executed trades for.

            period (optional): The period associated with the EPEX contract you would like executed trades for. If None and date input is of type datetime, the period is calculated.

        Raises:
            `TypeError`: If the period is not an integer or if no period is given and date is not of type datetime.

        """
        endpoint = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/Trades"

        period = get_period(date, period)
        date = convert_datetime_to_iso(date)

        request_details = {"Type": type, "Date": date, "Period": period}

        response = await self._post_request_async(endpoint, request_details)
        return convert_response_to_df(response, index_on=-1)

    async def get_epex_order_book_async(self, type: str, date: datetime, period: int = None) -> dict[str, pd.DataFrame]:
        """Get order book of a contract,given the date, period and type

        The date and period can either be specified by a datetime or by passing in an optional period input with a date.
        If specified by a datetime, the closest period when rounding down to the nearest half an hour will be used.
        If specified by the period input, and the period should be an integer.

        Args:
            type: The type associated with the EPEX contract you would like executed trades for. The options are "HH", "1H", "2H", "4H", "3 Plus 4", "Overnight", "Peakload", "Baseload", "Ext. Peak".

            date: The date associated with the EPEX contract you would like executed trades for.

            period (optional): The period associated with the EPEX contract you would like executed trades for. If None and date input is of type datetime, the period is calculated.

        Raises:
            `TypeError`: If the period is not an integer or if no period is given and date is not of type datetime.

        """
        endpoint = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/OrderBook"

        period = get_period(date, period)
        date = convert_datetime_to_iso(date)

        request_details = {"Type": type, "Date": date, "Period": period}

        response = await self._post_request_async(endpoint, request_details)
        output: dict[str, pd.DataFrame] = {}
        for table_str, data in response["data"].items():
            output[table_str] = convert_dict_to_df(data)

        return output

    async def get_epex_order_book_by_contract_id_async(self, contract_id: int) -> dict[str, pd.DataFrame]:
        """Get EPEX order book by contract ID

        Args:
            contract_id `int`: The ID associated with the EPEX contract you would like the order book for.

        """
        endpoint = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/OrderBookFromContractId"

        request_details = {
            "ContractId": contract_id,
        }

        response = await self._post_request_async(endpoint, request_details)
        output: dict[str, pd.DataFrame] = {}
        for table_str, data in response["data"].items():
            output[table_str] = convert_dict_to_df(data)

        return output

    async def get_epex_contracts_async(self, date: datetime) -> pd.DataFrame:
        """Get EPEX contracts for a given day

        Args:
            date `datetime.datetime`: The date you would like all contracts for.

        Raises:
            `TypeError`: If the inputted date is not of type `date` or `datetime`.

        """
        endpoint = f"{constants.EPEX_BASE_URL}/EnactAPI/Data/Contracts"

        date = convert_datetime_to_iso(date)

        request_details = {
            "Date": date,
        }

        response = await self._post_request_async(endpoint, request_details)
        return convert_response_to_df(response)

    async def get_N2EX_buy_sell_curves_async(self, date: datetime) -> dict:
        """Get N2EX buy and sell curves for a given day.

        Args:
            date `datetime.datetime`: The date you would like buy and sell curves for.

        """
        endpoint = f"{constants.SERIES_BASE_URL}/api/NordpoolBuySellCurves"

        date = convert_datetime_to_iso(date)

        request_details = {
            "Date": date,
        }

        return await self._post_request_async(endpoint, request_details)

    async def get_day_ahead_data_async(
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
            from: The start of the date range to compare against.

            to: The end of the date range for days to compare against.

            aggregate (optional): If set to true, the EFA blocks are considered as a single time range.

            numberOfSimilarDays (optional): The number of the most similar days to include in the response.

            selectedEfaBlocks (optional): The EFA blocks to find similar days for.

            seriesInput (optional):The series to find days with similar values to. The array must only contain strings from the following list: "ResidualLoad", "Tsdf", "WindForecast", "SolarForecast"
            "DynamicContainmentEfa", "DynamicContainmentEfaHF", "DynamicContainmentEfaLF", "DynamicRegulationHF", "DynamicRegulationLF", "DynamicModerationLF", "DynamicModerationHF", "PositiveBalancingReserve",
            "NegativeBalancingReserve", "SFfr". If none are specified, then all of the series listed are used in the calculation.
        Raises:
            `TypeError`: If the input dates are not of type date or datetime.

        """
        endpoint = f"{constants.MAIN_BASE_URL}/EnactAPI/DayAhead/data"

        fromDateString = convert_datetime_to_iso(fromDate)
        if toDate != None:
            toDateString = convert_datetime_to_iso(toDate)
        else:
            toDateString = None

        request_details = {
            "from": fromDateString,
            "to": toDateString,
            "aggregate": aggregate,
            "numberOfSimilarDays": numberOfSimilarDays,
            "selectedEfaBlocks": selectedEfaBlocks,
            "seriesInput": seriesInput,
        }

        response = await self._post_request_async(endpoint, request_details)
        output: dict[int, pd.DataFrame] = {}

        for key, value in response["data"].items():
            data_list = []
            for item in value:
                day = item["day"]
                score = item["score"]
                raw_data = item["rawData"]
                raw_data["day"] = day
                raw_data["score"] = score
                data_list.append(raw_data)
            df = pd.DataFrame(data_list)
            if not df.empty:
                df.set_index("score", inplace=True)
            output[int(key)] = df
        return output
