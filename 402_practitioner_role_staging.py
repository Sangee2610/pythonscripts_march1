import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""DROP TABLE IF EXISTS prd_Staging_Practitioner_role;
CREATE TABLE prd_Staging_Practitioner_role as
SELECT '' as Name,
(CASE WHEN LPR.ContactKey = '' THEN NULL ELSE CAST(LPR.ContactKey as INT) END) as ContactKey,
csf.id as Contact,
Role,
AcquistionDate,
ExpiryDate,
Active,
MaximumCouple,
MaximumPeople,
'' as DataSource,
'' as Owner_
FROM Landing_Practitioner_Role LPR
left outer join prd_staging_contactkey_id csf on csf.contactkey = LPR.contactkey::int
"""
)

conn.commit()
conn.close()
