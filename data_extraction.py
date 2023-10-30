import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import tabula
import yaml
import requests
import boto3
import awswrangler as wr


class DataExtractor:

    # given a DatabaseConnector, this initializes the corresponding engine and returns a table from the corresponding database with a given name.
    def read_rds_table(self, dbc: DatabaseConnector, table_name: str):
        engine = dbc.init_db_engine()
        return pd.read_sql_table(table_name, engine)
    

    # generates a dataframe from a pdf from a link. a default link is given
    # tabula.read_pdf returns a list of dataframes, so we pick the first ones
    def retrieve_pdf_data(self, link: str = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        df_list = tabula.read_pdf(link, pages = 'all')
        df = pd.concat(df_list, ignore_index = True)
        return df
    
    def list_number_of_stores(self, endpoint: str, header: dict):
        response = requests.get(endpoint, headers=header)
        return response.json()['number_stores']

    def retrieve_stores_data(self, endpoint: str):
        with open('api_keys.yaml','r') as stream:
            header = yaml.safe_load(stream)
        number_of_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header)

        stores_list = []
        for i in range(0,number_of_stores):
            store_endpoint = endpoint + str(i)
            response = requests.get(store_endpoint, headers=header)
            stores_list.append(response.json())
        stores_data = pd.DataFrame(stores_list)
        return stores_data
    
    def extract_from_s3(self, address: str = 's3://data-handling-public/products.csv'):
        s3 = boto3.client('s3')
        df = pd.read_csv("s3://data-handling-public/products.csv")
        return df
    
    def extract_events_data(self, endpoint:str = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        response = requests.get('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        df = pd.DataFrame(response.json())
        return df

if __name__ == '__main__':
    dex = DataExtractor()
    dex.extract_from_s3()