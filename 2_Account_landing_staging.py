import psycopg2
import config as cfg
import csv
import pandas as pd
import numpy as np
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""DROP TABLE IF EXISTS prd_staging_accounts_household;
CREATE TABLE prd_Staging_Accounts_household as
SELECT '0120Y000000skphQAA' as record_type_id,
(CASE WHEN AccountKey = '' THEN NULL ELSE CAST(AccountKey as INT) END) as AccountKey,
spo.psaofficekey as PSAOfficeKey,
AccountName,
Region,
RegionArea,
spo.id as PSAOffice,
PrimaryContactNumber,
PrimaryContact,
(CASE WHEN AccountNumber = '' THEN NULL ELSE CAST(AccountNumber as INT) END) as AccountNumber,
BranchCode,
(CASE WHEN Branch = '' THEN NULL ELSE CAST(Branch as INT) END) as branchkey,
MailingStreet as BillingStreet,
MailingCity as BillingCity,
MailingState as BillingState,
MailingCountry as BillingCountry,
MailingPostCode as BillingPostCode,
(CASE WHEN MailingLatitude = '' THEN NULL ELSE CAST(MailingLatitude as FLOAT8) END) as BillingLatitude,
(CASE WHEN MailingLongditude = '' THEN NULL ELSE CAST(MailingLongditude as FLOAT8) END) as BillingLongitude,
Description,
Phone,
MobilePhone,
'0051v000005QT8qAAG' as Owner_,
'CARE' as datasource
FROM Landing_Accounts LA 
left outer join prd_staging_psa_office_id spo on spo.name = LA.PSAAreaCode
where Account = 'Household' """)

cur.execute("""DROP TABLE IF EXISTS prd_Staging_Accounts_branch;
CREATE TABLE prd_Staging_Accounts_branch as
SELECT '0120Y000000skphQAA' as record_type_id,
(CASE WHEN AccountKey = '' THEN NULL ELSE CAST(AccountKey as INT) END) as AccountKey,
spo.psaofficekey as PSAOfficeKey,
AccountName,
Region,
RegionArea,
spo.id as PSAOffice,
PrimaryContactNumber,
PrimaryContact,
(CASE WHEN AccountNumber = '' THEN NULL ELSE CAST(AccountNumber as INT) END) as AccountNumber,
BranchCode,
(CASE WHEN Branch = '' THEN NULL ELSE CAST(Branch as INT) END) as branchkey,
MailingStreet as BillingStreet,
MailingCity as BillingCity,
MailingState as BillingState,
MailingCountry as BillingCountry,
MailingPostCode as BillingPostCode,
(CASE WHEN MailingLatitude = '' THEN NULL ELSE CAST(MailingLatitude as FLOAT8) END) as BillingLatitude,
(CASE WHEN MailingLongditude = '' THEN NULL ELSE CAST(MailingLongditude as FLOAT8) END) as BillingLongitude,
Description,
Phone,
MobilePhone,
'0051v000005QT8qAAG' as Owner_,
'CARE' as datasource
FROM Landing_Accounts LA 
left outer join prd_staging_psa_office_id spo on spo.name = LA.PSAAreaCode
where account = 'Branch' """)

conn.commit()
conn.close()
