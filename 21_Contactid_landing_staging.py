import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_staging_contactkey_id;
CREATE TABLE prd_staging_contactkey_id as
SELECT
(case when accountkey = '' then null else cast(CAST(accountkey as FLOAT8) as INT) end) as accountkey,
accountid as accountid,
(case when contactkey = '' then null else cast(CAST(contactkey as FLOAT8) as INT) end) as contactkey,
id as id
FROM prd_Landing_contactkey_id
""")


conn.commit()
