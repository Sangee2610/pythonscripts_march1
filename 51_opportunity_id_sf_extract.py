# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 16:53:28 2018

@author: css113429
"""
import json
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
from time import sleep
from salesforce_bulk import CsvDictsAdapter
import unicodecsv
import csv
import pandas as pd
import numpy as np
import config as cfg

##Salesforce table extraction
bulk = SalesforceBulk(username=cfg.USERNAME, 
	password=cfg.PASSWORD, 
	security_token=cfg.SECURITY_KEY, sandbox=True)

#Source CSV File path for Account
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/SalesForce_opportunity_id_extract.csv"

sf_fields = ['Opportunity_Key__c', 'Contact_Key__c', 'Recurring_Donations_Key__c', 'Mandate_Key__c', 
            'Payment_Profile_Key__c', 'npe03__Recurring_Donation__c', 'Contact__c', 
            'npsp4hub__Mandate__c', 'npsp4hub__Payment_Profile__c', 'Id']
#Target SFDC Object name
target_obj = "Opportunity"

# Extract the data from salesforce and save it to csv

job = bulk.create_query_job(target_obj, contentType='CSV')
sql = "SELECT " + ",".join(sf_fields) + " FROM " + target_obj
batch = bulk.query(job, sql)
bulk.close_job(job)
while not bulk.is_batch_done(batch):
    sleep(10)

for result in bulk.get_all_results_for_query_batch(batch):
    reader = unicodecsv.DictReader(result, encoding='utf-8')
    with open(input_file, 'a') as f:
    	pd.DataFrame(list(reader)).to_csv(f, header=True,index=False)


import psycopg2
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_landing_opportunity_id;
CREATE TABLE prd_landing_opportunity_id(
opportunity_key text,
contactkey text,
recurring_donations_key text,
mandatekey text,
paymentprofile_key text,
recurring_donation text,
contact text,
mandate text,
paymentprofile text,
id text
)
""")

ddsample = '/home/baadmin/NCT_ETL/input_files/pg_extract_prd/SalesForce_opportunity_id_extract.csv'

def data_cleaning_loading(filename):
    new_filename = filename.replace(".csv", "_corrected.csv")
    f = open(filename, encoding="ISO-8859-1")
    g = open(new_filename, "w+",encoding="ISO-8859-1")
    new_rows = []
    changes = { ',' : '',}

    for row in csv.reader(f, quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL, skipinitialspace=True):     # iterate over the rows in the file
        new_row = row      # at first, just copy the row
        for key, value in changes.items(): # iterate over 'changes' dictionary
            new_row = [ x.replace(key, value) for x in new_row ] # make the substitutions
        new_rows.append(new_row) # add the modified rows
    new_rows = new_rows[1:] #Remove header
    for new_row in new_rows:
        g.write(str(",".join(new_row)) + "\n")
    g.close()
    g = open(new_filename)
    cur.copy_from(g, 'prd_landing_opportunity_id',  sep=",")
    conn.commit()
    g.close()
    f.close()
    
data_cleaning_loading(ddsample)

#conn.close() 

conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_staging_opportunity_id;
CREATE TABLE prd_staging_opportunity_id AS
SELECT 
(CASE WHEN contactkey = '' THEN NULL ELSE CAST(CAST(contactkey as FLOAT8) as INT) END) as contactkey,
(CASE WHEN mandatekey = '' THEN NULL ELSE CAST(CAST(mandatekey as FLOAT8) as INT) END) as mandatekey,
(CASE WHEN opportunity_key = '' THEN NULL ELSE CAST(CAST(opportunity_key as FLOAT8) as INT) END) as opportunity_key,
(CASE WHEN paymentprofile_key = '' THEN NULL ELSE CAST(CAST(paymentprofile_key as FLOAT8) as INT) END) as paymentprofile_key,
(CASE WHEN recurring_donations_key = '' THEN NULL ELSE CAST(CAST(recurring_donations_key as FLOAT8) as INT) END),
contact,
mandate,
paymentprofile,
recurring_donation,
id
from prd_landing_opportunity_id
""")

conn.commit()
conn.close()

