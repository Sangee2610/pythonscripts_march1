import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
  
drop table if exists prd_staging2_contact;
CREATE TABLE prd_Staging2_contact as
SELECT c.contactkey, c.accountkey,  f.id as account, c.title, c.salutation, c.first_name, 
       c.last_name, c.prefered_first_name, c.branchkey, f.branch, c.gender, c.email, c.email_source_date, 
       c.volunteer, c.staff, c.accountholdername, c.bankname, c.accountnumber, 
       c.sortcode, c.practitioner, c.phone_1, c.phone_2, c.phone_2_date, c.edd, 
       c.source, c.sourcecode, c.mailingstreet, c.mailingcity, c.mailingstate, 
       c.mailingcountry, c.mailingpostalcode, c.mailinglatitude, c.mailinglongitude, 
       c.include, c.owner_, 
       (case when r.secondcontactkey > 0  then '0120Y0000005c3uQAA' else '0121v000000NO4sAAG' end) as record_type_id,
      'CARE' as datasource
  FROM prd_Staging1_Contact c
  left outer join
  prd_Staging_RecurringDonations r
  on
  c.contactkey = r.secondcontactkey
  inner join
  prd_Staging_accountkey_id_full_account f
  on
  c.accountkey = f.accountkey;

""")

conn.commit()
