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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/Salesforce_Practitioner_role.csv"

sf_fields = ['Name__c', 'ContactKey__c', 'Contact__c', 
            'Role__c', 'Acquisition_Date__c', 'Expiry_Date__c', 
            'Active__c', 'Maximum_Couple__c', 'Maximum_People__c','Id']


#Target SFDC Object name
target_obj = "Practitioner_Role__c"

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
DROP TABLE IF EXISTS prd_landing_Practitioner_role_id;
CREATE TABLE prd_landing_Practitioner_role_id(
Name text,
ContactKey text,
Contact text,
Role text,
acquistiondate text,
ExpiryDate text,
Active text,
MaximumCouple text,
MaximumPeople text,
id text
)
""")

ddsample = '/home/baadmin/NCT_ETL/input_files/pg_extract_prd/Salesforce_Practitioner_role.csv'

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
    cur.copy_from(g, 'prd_landing_Practitioner_role_id',  sep=",")
    conn.commit()
    g.close()
    f.close()
    
data_cleaning_loading(ddsample)

#conn.close() 

conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_staging_Practitioner_role_id;
CREATE TABLE prd_staging_Practitioner_role_id AS
SELECT 
Name,
ContactKey,
Contact,
Role,
acquistiondate,
ExpiryDate,
Active,
MaximumCouple,
MaximumPeople,
id,
'CARE' as datasource
from prd_landing_Practitioner_role_id
""")

conn.commit()
conn.close()

