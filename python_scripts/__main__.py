import os
import numpy as np
import pandas as pd

from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor


if __name__ == '__main__':

    abspath = os.path.dirname(__file__)
    credentials_file = os.path.join(abspath, '..', 'yaml_files','db_creds.yaml')
    
    db_connector = DatabaseConnector(credentials_file)
    dex = DataExtractor()
    d_cleaner = DataCleaning()

    user_df = dex.read_rds_table(db_connector,'legacy_users')
    user_df = d_cleaner.clean_user_data(user_df)
    db_connector.upload_to_db(user_df, 'dim_users')

    card_df = dex.retrieve_pdf_data()
    card_df = d_cleaner.clean_card_data(card_df)
    db_connector.upload_to_db(card_df, 'dim_card_details')

    store_df = dex.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')
    store_df = d_cleaner.clean_store_data(store_df)
    db_connector.upload_to_db(store_df, 'dim_store_details')

    product_df = dex.extract_from_s3()
    product_df = d_cleaner.clean_products_data(product_df)
    db_connector.upload_to_db(product_df, 'dim_products')

    orders_df = dex.read_rds_table(db_connector, 'orders_table')
    orders_df = d_cleaner.clean_orders_data(orders_df)
    db_connector.upload_to_db(orders_df, 'orders_table')

    events_df = dex.extract_events_data()
    events_df = d_cleaner.clean_events_data(events_df)
    db_connector.upload_to_db(events_df, 'dim_date_times')
