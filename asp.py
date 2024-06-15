#How to Alter a table
import pandas as pd
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

 # load credentials 
credentials = service_account.Credentials.from_service_account_file(
    '[link/to/json/file]') 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '[link/to/json/file]' 

def pullDataFromBQ(query):
    project_id = 'project_name'
    df = pd.io.gbq.read_gbq(query, project_id=project_id, dialect='standard')
    return df

# Replace 'your_project_id', 'your_dataset_id', 'your_table_id', and 'column_to_drop' with your values
project_id = 'your_project_id'
dataset_id = 'your_dataset_id'
table_id = 'your_table_id'
column_to_drop = 'your_column_to_drop'

# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Construct the SQL query to drop the column
sql_query = f"""
ALTER TABLE `{project_id}.{dataset_id}.{table_id}`
DROP COLUMN {column_to_drop}
"""
# Execute the SQL query
query_job = client.query(sql_query)
# Wait for the query to finish
query_job.result()
print(f"Column '{column_to_drop}' dropped from table '{table_id}' in BigQuery.")
 #OR
q = f"""ALTER TABLE {project_id}{dataset_id}{table_id}
       DROP COLUMN name"""
m = pullDataFromBQ(q)

#To add columns
q = f"""
ALTER TABLE {project_id}{dataset_id}{table_id}
ADD COLUMN name STRING"""  # add column and datatype
res = pullDataFromBQ(q)

df.to_gbq(f"{project_id}.{dataset_id}.{table_id}", project_id='project_id',
if_exists='append'

)
# To Delete with specific requirement from table.
DELETE
FROM {{project_id}{dataset_id}{table_id}
WHERE column name between 'start_date' and 'end_date' # modify the where clause to meet certain requirements.

#To Delete table
DROP TABLE {project_id}.{dataset_id}.{table_id};

