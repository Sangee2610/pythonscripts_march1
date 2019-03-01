import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv


tablename = "prd_Staging2_Payment"

query = "SELECT * from {} ".format(tablename)

outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

with open('/home/baadmin/NCT_ETL/input_files/pg_extract_prd/staging2_payment.csv', 'w') as f:
    cur.copy_expert(outputquery, f)

conn.commit()
conn.close()
