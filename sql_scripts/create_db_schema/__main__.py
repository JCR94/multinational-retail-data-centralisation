import os
import pandas as pd

import yaml

from operator import itemgetter
from sqlalchemy import create_engine
from sqlalchemy import text

if __name__ == '__main__':
    if os.path.exists('yaml_files'):
        credentials_file = os.path.join('yaml_files', 'local_db_creds.yaml')
    elif os.path.exists(os.path.join('..', 'yaml_files', 'local_db_creds.yaml')):
        credentials_file = os.path.join('..', 'yaml_files', 'local_db_creds.yaml')
    else:
        credentials_file = os.path.join('..', '..', 'yaml_files', 'local_db_creds.yaml')

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
            if os.path.exists(script):
                script_path = script
            elif os.path.exists(os.path.join('create_db_schema', script)):
                script_path = os.path.join('create_db_schema', script)
            else:
                script_path = os.path.join(list_of_sql_scripts, 'create_db_schema', script)
            with open(script_path) as file:
                query = text(file.read())
                connection.execute(query)