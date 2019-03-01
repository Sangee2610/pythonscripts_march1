import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_staging_mandate_Id;
CREATE TABLE prd_staging_mandate_Id as
SELECT
(CASE WHEN directdebitkey = '' then NULL else cast(CAST(directdebitkey as FLOAT) as INT) END) as directdebitkey,
(CASE WHEN MandateKey = '' then NULL else cast(CAST(MandateKey as FLOAT) as INT) END) as MandateKey,
Id,
(CASE WHEN paymentprofile_key = '' then NULL else cast(CAST(paymentprofile_key as FLOAT) as INT) END)as paymentprofile_key,
paymentprofile_id,
(CASE WHEN ContactKey = '' then NULL else cast(CAST(ContactKey as FLOAT) as INT)  END) as ContactKey
FROM prd_Landing_mandate_Id
""")

conn.commit()
conn.close()
