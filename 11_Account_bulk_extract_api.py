import json
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
from time import sleep
from salesforce_bulk import CsvDictsAdapter
import pandas as pd 
import unicodecsv
import config as cfg

#Authentication

bulk = SalesforceBulk(username=cfg.USERNAME, 
	password=cfg.PASSWORD, 
	security_token=cfg.SECURITY_KEY, sandbox=True)

#Source CSV File path for Account
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/Accountid_full_extract.csv"

#Target SFDC Object name
target_obj = "Account"

# Mapping of Input csv Fields to SalesForce Fields

sf_fields = ['Account_Key__c', 'Branch__c', 'Id']


# Extract the data from salesforce and save it to csv

job = bulk.create_query_job(target_obj, contentType='CSV')
sql = "SELECT " + ",".join(sf_fields) + " FROM " + target_obj
batch = bulk.query(job, sql)
bulk.close_job(job)
while not bulk.is_batch_done(batch):
    sleep(10)

for result in bulk.get_all_results_for_query_batch(batch):
    reader = unicodecsv.DictReader(result, encoding='UTF-8')
    with open(input_file, 'a') as f:
    	pd.DataFrame(list(reader)).to_csv(f, header=False, float_format='string')
    
