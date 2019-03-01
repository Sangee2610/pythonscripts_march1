import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_accountkey_id_branch;
CREATE TABLE prd_Staging_accountkey_id_branch as
SELECT
cast(CAST(accountkey as FLOAT) as INT) as accountkey,
Id,
'CARE' as datasource
FROM prd_Landing_Accountkey_id_branch
where length(accountkey) > 1
""")

conn.commit()
