import requests
import json
import math
import os
import pandas as pd
from pandas import DataFrame
from typing import Callable
import logging
from functools import wraps

logging.basicConfig(
    filename='log.txt',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)


class WorldBankData:
    # base api url that is used to query WB database
    base_api_url = "http://api.worldbank.org/v2"

    def __init__(self, indicator: str = 'SP.POP.TOTL', country: str = 'all',
                 date: str = '1960:2019', file_name: str = None,
                 over_write: bool = False, **args):
        """
        Class used to interact with World Bank database
        Args:
             indicator [str]: string representing the indicator to be extracted
             country [str]: country to extract the data for. If is empty,
                all countries will be considered
             args [dict]: additional filter to apply
        """
        logger.info('Initiating WoldBankData object')
        self.indicator = indicator
        self.country = country
        if not file_name:
            file_name = self.define_file_name()
        elif 'json' not in file_name.lower():
            file_name = f'{file_name}.json'
        self.file_name = file_name
        logger.info(f'The data will be saved in "{self.file_name}"')
        if self.check_file_exist(self.file_name) and not over_write:
            error_message = f'File already exists. Please delete it or set ' \
                            f'"over_write" to "True"'
            logger.error(error_message)
            raise FileExistsError(error_message)

        self.date = date
        self.args = args
        self.url = self._create_url()
        self.data = None
        self.data_downloaded = False
        self.data_transformed = False

    def exception(func: Callable):
        """

        Return:
             wrapper function
        """
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as error:
                logger.error(str(error))

        return wrapper

    @exception
    def _create_url(self) -> str:
        """
        Used to create base url for querying WB database.

        Return:
             string of the base url
        """
        logger.info('Setting-up base url')
        if self.indicator.lower() == 'all':
            url = f'{self.base_api_url}/indicator'
        elif self.indicator.lower() == 'source':
            url = f'{self.base_api_url}/source'
        else:
            url = f'{self.base_api_url}/country/{self.country}/indicator' \
               f'/{self.indicator}'
        logger.info(f'Base url: {url}')
        return url

    @exception
    def get(self) -> None:
        """
        Used to get the data from World Bank database. This function need to be
        called in order to get back the required data.

        Return:
             None
        """
        logger.info('Starting getting data from World Bank database')
        n_pages = 0

        def get_data(page: int = 1, per_page: int = 1):
            nonlocal n_pages
            params = {
                'format': 'json',
                'per_page': per_page,
                'page': page,
                **self.args
            }
            if self.indicator.lower() not in ['all', 'source']:
                params['date'] = self.date

            r = requests.get(
                url=self.url,
                params=params
            )
            logger.info(f'Pages {page} out of {n_pages}, from: {r.url}')
            return r.json()

        total = int(get_data()[0]['total'])

        per_page = 1000
        n_pages = math.ceil(total / per_page)

        logger.info(
            f'Summary: total records to download: {total}, '
            f'number of pages: {n_pages}'
        )
        if n_pages > 1:
            logger.warning('High number of records. It might take a few '
                           'minutes.')

        data = []
        first_round = True
        for page in range(1,  n_pages+1):
            output = get_data(page=page, per_page=1000)
            if first_round:
                data = output.copy()
                first_round = False
            else:
                data[1].extend(output[1])

        self.data = data
        self.data_downloaded = True

    @exception
    def save(self, file_name: str = None) -> None:
        """
        Function used to save the extracted data to a json file
        Args:
            file_name [str]: file name, without extension
        Return:
             None
        """
        if not self.data_downloaded:
            error_message = f'The data was not yet downloaded. Please run ' \
                            f'{self.__class__}.get() first'
            raise ValueError(error_message)

        if self.data_transformed:
            logger.info(f'Saving transformed data to file - {file_name}')
            self.data.to_json(self.file_name, orient='records',
                              lines=True)
        else:
            logger.info(f'Saving data to file - {file_name}')
            with open(self.file_name, 'w', encoding='utf8') as file:
                json.dump(self.data, file, indent=4, ensure_ascii=False)

    @exception
    def define_file_name(self):
        return f'{self.indicator}_{self.country}.json'

    @exception
    def transform_data(self) -> DataFrame:
        """
        Transform extracted data into a pandas DataFrame

        Return:
             None
        """
        logger.info(f'Transforming {self.indicator} data')
        headers = self.data[1][0].keys()
        df = pd.DataFrame(columns=headers)

        for item in self.data[1]:
            df = df.append(pd.DataFrame(item, columns=headers).loc['value', :],
                           ignore_index=True)

        self.data = df
        self.data_transformed = True

        return df

    @staticmethod
    def get_indicators_list():
        logger.info('Getting indicators data')
        data = WorldBankData(indicator='all')
        data.get()
        data.save('indicators')

    @staticmethod
    def get_sources_list():
        logger.info('Getting sources data')
        data = WorldBankData(indicator='source')
        data.get()
        data.save('sources')

    @staticmethod
    def check_file_exist(file_name):
        return os.path.exists(file_name)
