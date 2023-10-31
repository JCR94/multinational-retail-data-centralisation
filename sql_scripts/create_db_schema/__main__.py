import os
import pandas as pd

import yaml

from operator import itemgetter
from sqlalchemy import create_engine
from sqlalchemy import text

if __name__ == '__main__':
    abspath = os.path.dirname(__file__)
    credentials_file = os.path.join(abspath, '..', '..', 'yaml_files','local_db_creds.yaml')

    with open(credentials_file) as stream:
        credentials = yaml.safe_load(stream)

    HOST, USER, PASSWORD, DATABASE, PORT, DATABASE_TYPE, DBAPI = itemgetter('HOST', 'USER', 'PASSWORD', 'DATABASE', 'PORT', 'DATABASE_TYPE', 'DBAPI')(credentials)
    engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

    list_of_sql_scripts = ['cast_orders_table.sql',
                       'cast_dim_users.sql',
                       'cast_dim_store_details.sql',
                       'add_weight_class_dim_products.sql',
                       'cast_dim_products.sql',
                       'cast_dim_date_times.sql',
                       'cast_dim_card_details.sql',
                       'add_primary_keys.sql',
                       'add_foreign_keys.sql'
                       ]

    with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as connection:
        for script in list_of_sql_scripts:
            script_path = os.path.join(abspath, script)
            with open(script_path) as file:
                query = text(file.read())
                connection.execute(query)