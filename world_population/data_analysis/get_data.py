import requests
import json
import pandas as pd
from pandas import DataFrame


class WorldBankData:

    # base api url that is used to query WB database
    base_api_url = "http://api.worldbank.org/v2/country"

    def __init__(self, indicator: str = 'SP.POP.TOTL', country: str = 'all',
                 **args):
        """
        Class used to interact with World Bank database
        Args:
             indicator [str]: string representing the indicator to be extracted
             country [str]: country to extract the data for. If is empty,
                all countries will be considered
             args [dict]: additional filter to apply
        """
        self.indicator = indicator
        self.country = country
        self.args = args
        self.url = self._create_url()
        self.data = None
        self.data_downloaded = False
        self.data_transformed = False

    def _create_url(self) -> str:
        """
        Used to create base url for the query, using user input.

        Return:
             string of the base url
        """
        return f'{self.base_api_url}/{self.country}/indicator/{self.indicator}'

    def get(self) -> None:
        """
        Used to get the data from World Bank database. This function need to be
        called in order to get back the required data.

        Return:
             None
        """
        r = requests.get(
            url=self.url,
            params={
                'date': '1960:2019',
                'format': 'json',
                'per_page': '1000',
                'pages': '1000',
                **self.args
            }
        )

        self.data = r.json()
        self.data_downloaded = True

    def save(self, file_name) -> None:
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
            self.data.to_json(f'{file_name}.json', orient='records',
                              lines=True)
        else:
            with open(f'{file_name}.json', 'w') as file:
                json.dump(self.data, file, indent=4, ensure_ascii=False)

    def transform_data(self) -> DataFrame:
        """
        Transform extracted data into a pandas DataFrame

        Return:
             None
        """
        headers = self.data[1][0].keys()
        df = pd.DataFrame(columns=headers)

        for item in self.data[1]:
            df = df.append(pd.DataFrame(item, columns=headers).loc['value', :],
                           ignore_index=True)

        self.data = df
        self.data_transformed = True

        return df
