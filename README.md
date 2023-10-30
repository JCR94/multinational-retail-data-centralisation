# Multinational Retail Data Centralisation

In this project, we simulate a working environment in which we work for a multinational company that sells various goods across the globe.

### Table of contents:
- [Project description](#project-description)
- [Installation requirements](#installation-requirements)
- [How to use](#how-to-use)
- [File structure](#file-structure)
- [License information](#license-information)

## Project description

This project is part of the AICore immersive course in data engineering. The goal of this project is to extract data from multiple different sources, clean it, and collect it in a common database the make the data more accessible and readable by other team members.

The data revolves around the retail of a variety of products, and is spread across five tables as follows:
- dim_card_details
- dim_date_times
- dim_products
- dim_users
- orders_table

The data can then be used to answer a variety of business questions to help the company make more data-driven decisions.

## Installation requirements

The project requires a version of Python (preferably Python 3). If working offline, it also requires some relational database management system on your PC. We recommend using PostgreSQL through pgAdmin 4.

The following packages are required (we include the version numbers in parentheses, but the exact version might not be required):

- Pandas (2.1.1)
- NumPy (1.26.0)
- YAML (6.0.1)
- Requests (2.31.0)
- Tabula (2.8.2)
- Boto3 (1.28.66)
- AWS SDK for pandas (awswrangler) (3.4.1)
- SQLAlchemy (2.0.22)
- dateUtil (2.8.2)

Inbuilt modules that were used but don't require explicit installation are:

- RegEx (re) (2.2.1)
- Operator
- DateTime

## How to use

In addition to the files provided in the repo, the program requires the following YAML files:

- api_keys.yaml

   A YAML file of the form
   ```yaml
   x-api-key: 1234567890abcdefghijklmnopqrstuvwxyzABCD
   ```
   It is used to retrieve the store data from its database.

- db_creds.yaml

   A YAML file of the form
   ```yaml
   RDS_HOST: rds-host-name.123456789012.eu-west-1.rds.amazonaws.com
   RDS_PASSWORD: password
   RDS_USER: username
   RDS_DATABASE: postgres
   RDS_PORT: 5432
   ```
   It is used to connect to the RDS database to retrieve the user data.
   
- local_db_creds.yaml

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

To use, follow the following steps:

1. Create a database (e.g. through pgAdmin 4).
2. Create the YAML files above with the correct credentials.
3. If not connected yet, log in to the AWS CLI, which is necessary to retrieve one of the data points from an AWS Bucket.
4. Run the main.py file with
   ```bash
   python main.py
   ```

## File structure

```bash
.
├── __pycache__
│   ├── data_cleaning.cpython-311.pyc
│   ├── data_extraction.cpython-311.pyc
│   ├── database_utils.cpython-311.pyc
├── .gitignore
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── main.py
└── README.md

```

## License Information

TBD
