import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np


cur.execute("""DROP TABLE IF EXISTS prd_Staging_Mandate;
CREATE TABLE prd_Staging_Mandate as
SELECT '' as Name,
(CASE WHEN LM.ContactKey = '' THEN NULL ELSE CAST(LM.ContactKey as INT) END) as ContactKey,
SPP.PaymentProfileKey,
ROW_NUMBER () OVER (ORDER BY LM.ContactKey) as MandateKey,
null as DateSigned,
null as EndDate,
SPP.id as PaymentProfile,
'CARE N/A' as Type,
'BACS Direct Debit' as Target,
False as Active,
LM.Source as sourcecode,
LM.DDReference,
'0051v000005QT8qAAG' as Owner_,
(CASE WHEN LM.bookingKey = '' THEN NULL ELSE CAST(LM.bookingKey as INT) END) as bookingKey,
(CASE WHEN LM.DirectDebitKey = '' THEN NULL ELSE CAST(LM.DirectDebitKey as INT) END) as DirectDebitKey,
'CARE' as datasource
FROM prd_Landing_Mandate LM
INNER JOIN 
prd_Staging_Paymentprofile_Id SPP
on
SPP.ContactKey = LM.ContactKey::int and SPP.DirectDebitKey = LM.DirectDebitKey::int
""")

conn.commit()
conn.close()
