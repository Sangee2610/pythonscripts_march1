import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np


cur.execute("""DROP TABLE IF EXISTS prd_Staging_Payment_Profile;
CREATE TABLE prd_Staging_Payment_Profile as
SELECT '' as Name,
null as AccountKey,
(CASE WHEN LPP.ContactKey = '' THEN NULL ELSE CAST(LPP.ContactKey as INT) END) as ContactKey,
ROW_NUMBER () OVER (ORDER BY LPP.ContactKey) as PaymentProfileKey,
null as Account,
(SCI.Id) as Contact,
LPP.AccountName as HolderName,
null as BankName,
(CASE WHEN LPP.AccountNumber = '' THEN NULL ELSE CAST(LPP.AccountNumber as INT) END) as AccountNumber,
(CASE WHEN LPP.SortCode = '' THEN NULL ELSE CAST(LPP.SortCode as INT) END) as SortCode,
(CASE WHEN LPP.StartDate = '' THEN NULL ELSE TO_CHAR(TO_TIMESTAMP(LPP.StartDate, 'YYYY-MM-DD HH24:MI:SS') at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) as StartDate,
False as Active,
LPP.Source as sourcecode,
'0051v000005QT8qAAG' as Owner_,
(CASE WHEN LPP.bookingKey = '' THEN NULL ELSE CAST(LPP.bookingKey as INT) END) as bookingKey,
(CASE WHEN LPP.DirectDebitKey = '' THEN NULL ELSE CAST(LPP.DirectDebitKey as INT) END) as DirectDebitKey,
LPP.DDReference,
'CARE' as datasource
FROM prd_Landing_Payment_Profile LPP
INNER JOIN 
(SELECT i.*, row_number() over (partition by contactkey order by accountkey desc) as seqnum
  FROM prd_staging_contactkey_id i) SCI 
on 
SCI.ContactKey = LPP.ContactKey::int
and seqnum = 1
WHERE LPP.ContactKey::int NOT IN (1779527, 1724728, 1725948, 1725948, 1697016, 1766605)""")

conn.commit()
conn.close()
