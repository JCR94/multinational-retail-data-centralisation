import pandas as pd
import yaml
from sqlalchemy import create_engine
from operator import itemgetter # only needed to access multiple key-values at once in a single line
from sqlalchemy import inspect # only there for test purposes, might not be needed
from sqlalchemy import text

class DatabaseConnector:

    # initialises the class by choosing a credentials file
    def __init__(self, credentials_file = 'db_creds.yaml'):
        self.credentials_file = credentials_file

    # reads the yaml file given by the credentials name, and returns it as a dictionary with the credentials.
    def read_db_creds(self):
        with open(self.credentials_file) as stream:
            data_loaded = yaml.safe_load(stream)
        return data_loaded

    # initializes and returns the engine that we will use to extract data. the databse uses the credentials given in this class.
    def init_db_engine(self):
        credentials = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST, USER, PASSWORD, DATABASE, PORT = itemgetter('RDS_HOST', 'RDS_USER', 'RDS_PASSWORD', 'RDS_DATABASE', 'RDS_PORT')(credentials)
        
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    # lists the tables in the database given by the engine.
    def list_db_tables(self):
        engine = self.init_db_engine()
        return inspect(engine).get_table_names()
    
    # uploads a datafram df to a database defined in local_db_creds.yaml and saves it under as a table with a given table name
    def upload_to_db(self, df: pd.DataFrame, table_name: str):
        
        with open('local_db_creds.yaml') as stream:
            credentials = yaml.safe_load(stream)

        HOST, USER, PASSWORD, DATABASE, PORT, DATABASE_TYPE, DBAPI = itemgetter('HOST', 'USER', 'PASSWORD', 'DATABASE', 'PORT', 'DATABASE_TYPE', 'DBAPI')(credentials)
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        df.to_sql(table_name, engine, if_exists='replace')