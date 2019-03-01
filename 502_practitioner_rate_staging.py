import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""DROP TABLE IF EXISTS prd_Staging_Partitioner_rate;
CREATE TABLE prd_Staging_Partitioner_rate as
SELECT '' as Name,
(CASE WHEN LPRa.ContactKey = '' THEN NULL ELSE CAST(LPRa.ContactKey as INT) END) as ContactKey,
csf.id as Contact,
PayBand,
Boundary,
null as StartDate,
null as EndDate,
Cost,
'CARE' as DataSource,
'' as Owner_
FROM Landing_Partitioner_Rate LPRa
left outer join prd_staging_contactkey_id csf on csf.contactkey = LPRa.contactkey::int

"""
)

conn.commit()
conn.close()
