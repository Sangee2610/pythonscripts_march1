import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np


cur.execute("""DROP TABLE IF EXISTS prd_Staging_Gift_Aid_Declaration;
CREATE TABLE prd_Staging_Gift_Aid_Declaration as
SELECT null as AccountKey,
(CASE WHEN LGD.ContactKey = '' THEN NULL ELSE CAST(LGD.ContactKey as INT) END) as ContactKey,
(CASE WHEN LGD.DeclarationNumber = '' THEN NULL ELSE CAST(LGD.DeclarationNumber as INT) END) as GiftAidDeclarationKey,
null as Account,
SC.Id as Contact,
False as Active,
null as ReasonforDeactivation,
'CARE N/A' as Type,
(CASE WHEN LGD.DeclarationNumber = '' THEN NULL ELSE CAST(LGD.DeclarationNumber as INT) END) as DeclarationNumber,
LGD.DeclarationDate,
LGD.SourceDescription as Source, 
(CASE WHEN LGD.Method = 'O' THEN 'Oral'
WHEN LGD.Method = 'E' THEN 'Electronic'
WHEN LGD.Method = 'W' THEN 'Written'
END) as AcquisitionMethod,
(CASE WHEN LGD.StartDate = '' THEN NULL ELSE TO_CHAR(TO_TIMESTAMP(LGD.StartDate, 
'YYYY-MM-DD HH24:MI:SS') at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')  END) as StartDate,
(CASE WHEN LGD.EndDate = '' THEN NULL ELSE TO_CHAR(TO_TIMESTAMP(LGD.EndDate, 
'YYYY-MM-DD HH24:MI:SS') at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')  END) as EndDate,
'0051v000005QT8qAAG' as Owner_,
LGD.Source as Sourcecode,
'CARE' as datasource
FROM prd_Landing_GiftAid_Declaration LGD
INNER JOIN 
(SELECT i.*, row_number() over (partition by contactkey order by accountkey desc) as seqn
 FROM prd_staging_contactkey_id i) SC 
on SC.ContactKey = LGD.ContactKey::int
and seqn = 1""")

conn.commit()
conn.close()
