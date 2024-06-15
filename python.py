# How to create dataset and table on Bigquery using python

import pandas as pd
import math as ma
import decimal
import numpy as np
from decimal import Decimal
from pymongo import MongoClient
import certifi
import os
import google.auth
from datetime import datetime, tzinfo, timezone
from google.oauth2 import service_account
from datetime import datetime, timedelta, date
from google.cloud import bigquery
import gspread
from pandas_gbq import to_gbq
from oauth2client.service_account import ServiceAccountCredentials

dataset_id = 'dataset'
project_id = 'project_name'
table_name = 'table_name'

 # load credentials 
credentials = service_account.Credentials.from_service_account_file(
    '[link/to/json/file]') 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '[link/to/json/file]' 

def pullDataFromBQ(query):
    project_id = 'project_name'
    df = pd.io.gbq.read_gbq(query, project_id=project_id, dialect='standard')
    return df

# Define the partition column
partition_column = 'created'

# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Define the destination table
destination_table = f'{project_id}.{dataset_id}.{table_name}'

table = bigquery.Table(destination_table)
table.schema = [
    bigquery.SchemaField('id', 'STRING'),
    bigquery.SchemaField('action', 'STRING'),
    bigquery.SchemaField('success', 'BOOLEAN'),
    bigquery.SchemaField('ref', 'STRING'),
    bigquery.SchemaField('gateway', 'STRING'),
    bigquery.SchemaField('service', 'STRING'),
    bigquery.SchemaField('created', 'TIMESTAMP'),
    bigquery.SchemaField('name', 'STRING'),
    bigquery.SchemaField('meter_no', 'STRING'),
    bigquery.SchemaField('customer_no', 'STRING'),
    bigquery.SchemaField('phone', 'STRING'),
    bigquery.SchemaField('address', 'STRING'),
    bigquery.SchemaField('vend_type', 'STRING'),
    bigquery.SchemaField('tariff', 'STRING'),
    bigquery.SchemaField('tariff_rate', 'STRING'),
    bigquery.SchemaField('debt', 'FLOAT') # Add more columns and datatypes.

]
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field=partition_column
)

# Create the table in BigQuery
table = client.create_table(table)

# Load the DataFrame into the partitioned table
job = client.load_table_from_dataframe(dat, table, job_config=bigquery.LoadJobConfig())

# Wait for the job to complete
job.result()

print(f'Table created in BigQuery: {destination_table}')
