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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/Salesforce_PSA_Office_extract.csv"

sf_fields = ['Name__c', 'PSA_Office_Key__c', 'Reference__c', 'Cash__c', 
            'Capacity_PSA__c', 'Capacity_PSA_Email__c', 'Enquiry_PSA__c', 
            'Enquiry_PSA_Email__c','Id']

#Target SFDC Object name
target_obj = "PSA_Office__c"

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

print("csv extracted")

import psycopg2
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_landing_psa_office_id;
CREATE TABLE prd_landing_psa_office_id(
Name text,
PSAOfficeKey text,
Reference text,
Cash text,
CapacityPSA text,
CapacityPSAEmail text,
EnquiryPSA text,
EnquiryPSAEmail text,
id text
)
""")

ddsample = '/home/baadmin/NCT_ETL/input_files/pg_extract_prd/Salesforce_PSA_Office_extract.csv'

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
    cur.copy_from(g, 'prd_landing_psa_office_id',  sep=",")
    conn.commit()
    g.close()
    f.close()
    
data_cleaning_loading(ddsample)

#conn.close() 

conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_staging_psa_office_id;
CREATE TABLE prd_staging_psa_office_id AS
SELECT 
Name,
PSAOfficeKey,
Reference,
Cash,
CapacityPSA,
CapacityPSAEmail,
EnquiryPSA,
EnquiryPSAEmail,
id,
'CARE' as datasource
from prd_landing_psa_office_id
""")

conn.commit()
conn.close()

