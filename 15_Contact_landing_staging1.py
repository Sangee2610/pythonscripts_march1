import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np


cur.execute("""
DROP TABLE IF EXISTS prd_Staging1_Contact;
CREATE TABLE prd_Staging1_Contact as
SELECT
(CASE WHEN ContactKey = '' THEN NULL ELSE CAST(ContactKey as INT) END) as ContactKey,
(CASE WHEN AccountKey = '' THEN NULL ELSE CAST(AccountKey as INT) END) as AccountKey,
NULL as Account,
Title as Salutation,
Salutation as Title,
First_Name,
Last_Name,
Prefered_First_Name,
(CASE WHEN Branch = '' THEN NULL ELSE CAST(Branch as INT) END) as BranchKey,
Gender,
Email,
(CASE WHEN Email_Source_Date = '' THEN NULL ELSE TO_TIMESTAMP(Email_Source_Date, 'YYYY-MM-DD HH24:MI:SS') END) as Email_Source_Date,
(CASE WHEN Volunteer = 'Y' THEN 'true' ELSE 'false' END) as Volunteer,
(CASE WHEN Staff = '1' THEN 'true' ELSE 'false' END) as Staff,
NULL as AccountHolderName,
NULL as BankName,
NULL as AccountNumber,
NULL as SortCode,
(CASE WHEN Practitioner = 'Y' THEN 'true' ELSE 'false' END) as Practitioner,
Phone_1,
(CASE WHEN Phone_1_Date = '' THEN NULL ELSE TO_TIMESTAMP(Phone_1_Date, 'YYYY-MM-DD HH24:MI:SS') END) as Phone_1_Date,
Phone_2,
(CASE WHEN Phone_2_Date = '' THEN NULL ELSE TO_TIMESTAMP(Phone_2_Date, 'YYYY-MM-DD HH24:MI:SS') END) as Phone_2_Date,
(CASE WHEN EDD = '' THEN NULL ELSE TO_TIMESTAMP(EDD, 'YYYY-MM-DD HH24:MI:SS') END) as EDD,
Source as sourcecode,
(CASE WHEN Source_Date = '' THEN NULL ELSE TO_TIMESTAMP(Source_Date, 'YYYY-MM-DD HH24:MI:SS') END) as Source_Date,
Source_Description as source,
mailingStreet,
mailingCity,
mailingState,
mailingCountry,
mailingPostalCode,
(CASE WHEN mailingLatitude = '' THEN NULL ELSE CAST(mailingLatitude as FLOAT8) END) as MailingLatitude,
(CASE WHEN mailinglongtitdude = '' THEN NULL ELSE CAST(mailinglongtitdude as FLOAT8) END) as MailingLongitude,
include,
'0051v000005QT8qAAG' as Owner_,
'CARE' as datasource
FROM Landing_Contact;
""")

conn.commit()
conn.close()
