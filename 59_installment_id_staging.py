# Creating table with ID in PostGress SQL to use it in next tab
import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_Installment_Id;
CREATE TABLE prd_Staging_Installment_Id as
SELECT
(CASE WHEN contact_key__c = '' then NULL else cast(CAST(contact_key__c as FLOAT) as INT) END) as ContactKey,
cpm__Contact__c as ContactId,
(CASE WHEN Installment_Key__c = '' then NULL else cast(CAST(Installment_Key__c as FLOAT) as INT) END) as InstallmentKey,
Id,
'CARE' as datasource
FROM prd_Landing_Installment_Id
""")

conn.commit()
conn.close()
