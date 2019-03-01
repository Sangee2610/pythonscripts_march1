import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_Paymentprofile_Id;
CREATE TABLE prd_Staging_Paymentprofile_Id as
SELECT
(CASE WHEN directdebitkey = '' then NULL else cast(CAST(directdebitkey as FLOAT) as INT) END) as directdebitkey,
(CASE WHEN PaymentProfileKey = '' then NULL else cast(CAST(PaymentProfileKey as FLOAT) as INT) END) as PaymentProfileKey,
Id,
(CASE WHEN ContactKey = '' then NULL else cast(CAST(ContactKey as FLOAT) as INT) END) as ContactKey,
Contact
FROM prd_Landing_Paymentprofile_Id
""")

conn.commit()
conn.close()
