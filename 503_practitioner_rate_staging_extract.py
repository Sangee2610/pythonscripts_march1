import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

tablename = "prd_Staging_Partitioner_rate"

query = "SELECT * from {} ".format(tablename)

outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

with open('/home/baadmin/NCT_ETL/input_files/pg_extract_prd/prd_Staging_Practitioner_rate_extract.csv', 'w') as f:
    cur.copy_expert(outputquery, f)

conn.commit()
conn.close()
