import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_PSA_Office;
CREATE TABLE prd_Staging_PSA_Office as
SELECT
Name,
(CASE WHEN PSAOfficeKey = '' THEN NULL ELSE CAST(PSAOfficeKey as INT) END) as PSAOfficeKey,
Reference,
(CASE WHEN Cash = '' THEN NULL ELSE CAST(Cash as INT) END) as Cash,
CapacityPSA,
CapacityPSAEmail,
EnquiryPSA,
EnquiryPSAEmail,
'CARE' as datasource,
''  as Owner_
FROM Landing_PSA_Office
""")

conn.commit()
conn.close()
