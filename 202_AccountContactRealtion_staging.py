
import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np


cur.execute("""
DROP TABLE IF EXISTS prd_Staging_AccountContactRelation;
CREATE TABLE prd_Staging_AccountContactRelation as
SELECT
(CASE WHEN AccountKey = '' THEN NULL ELSE CAST(AccountKey as INT) END) as AccountKey,
(CASE WHEN ContactKey = '' THEN NULL ELSE CAST(ContactKey as INT) END) as ContactKey,
(CASE WHEN Branch = '' THEN NULL ELSE CAST(Branch as INT) END) as BranchKey,
IsDirect,
Roles,
null as StartDate,
'CARE N/A' as Type,
'' as Owner_,
'CARE' as datasource,
FROM Landing_Accounts_Contact_relation
""")

conn.commit()
conn.close()
