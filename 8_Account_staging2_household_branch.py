import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Staging2_accounts_household_branch;
CREATE TABLE prd_Staging2_accounts_household_branch as
SELECT h.record_type_id, h.accountkey, h.accountname, h.region, h.regionarea, 
       h.psaoffice, h.primarycontactnumber, h.primarycontact, h.accountnumber, 
       h.branchcode, h.branchkey, h.billingstreet, h.billingcity, h.billingstate, 
       h.billingcountry, h.billingpostcode, h.billinglatitude, h.billinglongitude, 
       h.description, h.phone, h.mobilephone, h.owner_, b.Id as branch,'CARE' as datasource
  FROM prd_staging_accounts_household h
  left outer join
  (select distinct Id, accountkey from prd_staging_accountkey_id_branch) b
  on
  h.branchkey = b.accountkey ;
""")

conn.commit()
