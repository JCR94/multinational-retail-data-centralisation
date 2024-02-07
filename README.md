
# Multinational Retail Data Centralisation

In this project, we simulate a working environment in which we work for a multinational company that sells various goods across the globe.

### Table of contents:
- [Project description](#project-description)
- [Installation requirements](#installation-requirements)
   - [Modules](#modules)
   - [Files](#files)
- [How to use](#how-to-use)
	- [Alternative ''master'' main file](#alternative-master-main-file)
- [File structure](#file-structure)
- [Languages and modules used](#languages-and-modules-used)
   - [Languages](#languages)
   - [Modules](#modules-1)
- [License information](#license-information)

## Project description

This project is part of the AICore immersive course in data engineering. The goal of this project is to extract data from multiple different sources, clean it, and collect it in a common database to make the data more accessible and readable by other team members.

The data revolves around the retail of a variety of products, and is spread across six tables as follows:
- `dim_card_details`
- `dim_date_times`
- `dim_products`
- `dim_store_details`
- `dim_users`
- `orders_table`

The data is stored in a star-based schema with the `orders_table` as the center.

![EDR-image](https://images2.imgbox.com/08/47/vy4qWALs_o.png)

The data can then be used to answer a variety of business questions to help the company make more data-driven decisions.

The project can be divided into three main components:

1. Extraction, cleaning, and uploading of data to the database (handled by Python scripts inside the `python_scripts` directory).
2. Casting data to appropriate data types within the database, and adding primary and foreign keys to create star-based schema (handled by SQL scripts inside the `create_db_schema` directory).
3. Execute a variety of queries to obtain requested data (handled by SQL scripts inside the `queries` directory).

## Installation requirements

### Modules

The project requires a version of Python (preferably Python 3) to be installed. It also requires some relational database management system. We recommend installing pgAdmin 4 as this was used during testing, and as we used PostgreSQL as our database system.

The following 3<sup>rd</sup> party libraries are required (we include the version numbers in parentheses, but other versions may work too):

- Pandas (2.1.1)
- NumPy (1.26.0)
- YAML (6.0.1)
- Requests (2.31.0)
- Tabula (2.8.2)
- Boto3 (1.28.66)
- AWS SDK for pandas (awswrangler) (3.4.1)
- SQLAlchemy (2.0.22)
- dateutil (2.8.2)

The full list of modules that were used can be found [here](#modules-1).

### Files

In addition to the files provided in the repo, the program requires the following YAML files:

- `api_keys.yaml`

   A YAML file of the form
   ```yaml
   x-api-key: 1234567890abcdefghijklmnopqrstuvwxyzABCD
   ```
   It is used to retrieve the store data from its database.

- `db_creds.yaml`

   A YAML file of the form
   ```yaml
   RDS_HOST: rds-host-name.123456789012.eu-west-1.rds.amazonaws.com
   RDS_PASSWORD: password
   RDS_USER: username
   RDS_DATABASE: postgres
   RDS_PORT: 5432
   ```
   It is used to connect to the RDS database to retrieve the user data.
   
- `local_db_creds.yaml`

   A YAML file of the form
   ```yaml
   HOST: hostname
   PASSWORD: password
   USER: username
   DATABASE: databasename
   PORT: 5432
   DATABASE_TYPE: postgresql
   DBAPI: psycopg2
   ```
   It is used to connect to the database you want to store all your retrieved and cleaned data in.

These three files need to be in the `yaml_files` directory before running any Python files. You may use the templates provided. Simply open them in your favorite editor, modify the details, and rename the files to remove the `_template` qualifier (e.g. rename `api_keys_template.yaml` to `api_key.yaml`).


## How to use

To use the program, follow the following steps:

1. Create a database (e.g. through pgAdmin 4).
2. Create the [YAML files described above](#files) with the correct credentials.
3. If not connected yet, log in to the AWS CLI, which is necessary to retrieve one of the data points from an AWS Bucket.
4. Run the `__main.py__` file inside the `python_scripts` directory with
   ```bash
   python __main__.py
   ```
   or by running the `python` folder itself.
   ```bash
   python path/to/python_scripts
   ```
5. Run all the scripts in the `sql_scripts/create_db_schema` directory. This can be done by either manually running them one by one in the following order:
   1. `cast_orders_table.sql`
   2. `cast_dim_users.sql`
   3. `cast_dim_store_details.sql`
   4. `add_weight_class_dim_products.sql`
   5. `cast_dim_products.sql`
   6. `cast_dim_date_times.sql`
   7. `cast_dim_card_details.sql`
   8. `add_primary_keys.sql`
   9. `add_foreign_keys.sql`
   
   or by running the `__main.py__` file inside the `sql_scripts/create_db_schema` directory,
   ```bash
   python __main__.py
   ```
   or by running the `create_db_schema` folder itself.
   ```bash
   python path/to/create_db_schema
   ```
6. Finally, you can run any scripts in `sql_scripts/queries` to fetch relevant data.

### Alternative ''master'' main file

Alternatively to points `4.` and `5.`, you may either run the `__main__.py` file in the root directory of the project, or run the folder containing that file, instead. The top-level `__main__.py` file _(notated with an (*) in the file structure below)_ simply runs the two main files inside the `python_scripts` and `create_db_schema` directories in that order. E.g. if your file structure is

```bash
project_folder
├── python_scripts
│   └── __main__.py
├── sql_scripts
│   ├── create_db_schema
│   │   └── __main__.py
│   └── queries
├── yaml_files
└── __main__.py (*)
```
Then you may run:

```bash
python path/to/project_folder
```

or inside the `project_folder`:
```bash
python __main__.py
```

## File structure

```bash
.
├── python_scripts
│   ├── __main__.py
│   ├── data_cleaning.py
│   ├── data_extraction.py
│   └── database_utils.py
├── sql_scripts
│   ├── create_db_schema
│   │   ├── __main__.py
│   │   ├── add_foreign_keys.sql
│   │   ├── add_primary_keys.sql
│   │   ├── add_weight_class_dim_products.sql
│   │   ├── cast_dim_card_details.sql
│   │   ├── cast_dim_date_times.sql
│   │   ├── cast_dim_products.sql
│   │   ├── cast_dim_store_details.sql
│   │   ├── cast_dim_users.sql
│   │   └── cast_orders_table.sql
│   └── queries
│       ├── average_time_between_sales_per_year.sql
│       ├── online_vs_offline_sales.sql
│       ├── percentage_of_sales_per_store.sql
│       ├── staff_numbers_per_country_code.sql
│       ├── total_no_stores_by_country_id.sql
│       ├── total_no_stores_by_locality.sql
│       ├── total_sales_per_month.sql
│       ├── total_sales_per_store_type_in_germany.sql
│       └── total_sales_per_year_and_month.sql
├── yaml_files
│   ├── api_keys_template.yaml
│   ├── db_creds.yaml
│   └── loacal_db_creds_template.yaml
├── __main__.py
├── .gitignore
└── README.md
```

## Languages and modules used

### Languages
- Python
- PostgreSQL

### Modules

- OS
- DateTime
- Operator
- RegEx (re) (2.2.1)
- Pandas (2.1.1)
- NumPy (1.26.0)
- AWS SDK for pandas (awswrangler) (3.4.1)
- Boto3 (1.28.66)
- dateutil (2.8.2)
- Requests (2.31.0)
- SQLAlchemy (2.0.22)
- Tabula (2.8.2)
- YAML (6.0.1)

## License Information

TBD
