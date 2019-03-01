import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_accountkey_id_full_account;
CREATE TABLE prd_Staging_accountkey_id_full_account as
SELECT
(case when accountkey = '' then null else cast(CAST(accountkey as FLOAT8) as INT) end) as accountkey,
Branch,
Id
FROM prd_Landing_accountkey_id_full_account
""")

conn.commit()
