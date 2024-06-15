import pandas as pd
from pandas import DataFrame
from datetime import datetime, tzinfo, timezone
import json
import requests
import certifi
import mysql.connector

mydb = mysql.connector.connect(
    host=,
    user=,
    password=,
    database=",
    port = 
)
mycursor = mydb.cursor()
sp = pd.read_excel('excel file path')
sp.head() # to check the first 5 rows
sp.shape # to check the number of rows and columns.

order_status_out =[]
type_out =[]
vend_status_out =[]
amt =[]
count = 0
d_len = len(sp)
for ref in sp["ORDER ID"]:
  fetch_query = f"""
         select pt.order_status,tvr.type,if(tvr.payment_reference is null,'FAILED','SUCCESSFUL')tvr_status,pt.amount,
         from table1 pt
         left join table2 tvr on tvr.payment_reference = pt.order_id
         where pt.order_id = '{ref}'
         and pt.created_at >= '2020-01-01' """
  mycursor.execute(fetch_query)
  rows = mycursor.fetchall()
  try :
    order_status = rows[0][0]
    order_status_out.append(order_status)
    types = rows[0][1]
    type_out.append(types)
    vend_status = rows[0][2]
    vend_status_out.append(vend_status)
    amount = rows[0][3]
    amt.append(amount)
  except :
    "list index out of range"
    order_status_out.append(None)
    type_out.append(None)
    vend_status_out.append(None)
    amt.append(None)
  count += 1
  print(count, " out of ", d_len)

sp['order_status'] =order_status_out
sp['vend_status'] = vend_status_out
sp['types'] = type_out
sp['amount'] = amt
sp.tail()

sp.to_csv("name of file.csv")
!cp '/content/name of file.csv' '/content/drive/MyDrive' # to make a copy to drive.

#how to mearge with python.
# perform a left mearge, works same way with mysql left join
df = pd.merge(df1, df2, left_on='order_ID', right_on='order_ref', how='left')
df.head()
# to check difference in left_table from the right_table
merged_df = pd.merge(df1, df2, how='left', left_on='order_ID', right_on='order_ref', indicator=True)
df1_only = merged_df[merged_df['_merge'] == 'left_only']

# Reset the index if needed
df1_only = df1_only.reset_index(drop=True)
