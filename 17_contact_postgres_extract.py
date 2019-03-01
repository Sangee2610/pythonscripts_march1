import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

tablename = "prd_staging2_contact"

query = "SELECT * from {} ".format(tablename)

outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

with open('/home/baadmin/NCT_ETL/input_files/postgres_extracts/staging2_contact.csv', 'w') as f:
    cur.copy_expert(outputquery, f)

conn.commit()
conn.close()


