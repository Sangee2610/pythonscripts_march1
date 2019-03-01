import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_contact_sf_final;
create table prd_contact_sf_final as
SELECT 
c.contactkey, c.accountkey, c.account, c.title, c.salutation, c.first_name, 
     c.last_name, c.prefered_first_name, c.branchkey, c.branch, c.gender, c.email, 
     c.email_source_date, c.volunteer, c.staff, c.accountholdername, c.bankname, 
     c.accountnumber, c.sortcode, c.practitioner, c.phone_1, c.phone_2, c.phone_2_date, 
     c.edd, c.source, c.sourcecode, c.mailingstreet, c.mailingcity, c.mailingstate, 
     c.mailingcountry, c.mailingpostalcode, c.mailinglatitude, c.mailinglongitude, 
     c.include, c.owner_, c.record_type_id
  FROM
prd_staging_contactkey_id i
inner join
prd_staging2_contact c
on
i.contactkey = c.contactkey
where
i.contactkey is not null
""")

conn.commit()
