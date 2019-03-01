import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_PostCodeAllocationrelation;
CREATE TABLE prd_Staging_PostCodeAllocationrelation as
SELECT
(CASE WHEN AccountKey = '' THEN NULL ELSE CAST(AccountKey as INT) END) as AccountKey,
null as PostCodeAllocationKey,
Branch,
'' as Outcode,
'CARE' as datasource,
'' as Owner_
FROM Landing_PostcodeAllocation
""")

conn.commit()
conn.close()
