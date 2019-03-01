import json
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
from time import sleep
from salesforce_bulk import CsvDictsAdapter
import pandas as pd
import config as cfg
#Authentication

bulk = SalesforceBulk(username=cfg.USERNAME, 
	password=cfg.PASSWORD, 
	security_token=cfg.SECURITY_KEY, sandbox=True)

#Source CSV File path for Account
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/staging_giftaid_extract.csv"

#Target SFDC Object name
target_obj = "gaid__Gift_Aid_Declaration__c"

# Mapping of Input csv Fields to SalesForce Fields

sf_mapping = {'contactkey': 'ContactKey__c',
              'giftaiddeclarationkey': 'GiftAid_Declaration_Key__c',
              'contact': 'gaid__Contact__c',
              'active': 'gaid__Active__c',
              'type': 'gaid__Type__c',
              'declarationnumber': 'Declaration_Number__c',
              'declarationdate': 'Declaration_Date__c',
              'source': 'Source__c',
              'startdate': 'gaid__Start_Date__c',
              'enddate': 'EndDate__c',
              'owner_': 'Owner__c',
              'acquisitionmethod': 'Method__c',
              'sourcecode': 'Source_Code__c',
              'datasource': 'Data_Source__c'
              }

# Read the input file in batches and upload to SalesForce
for gm_chunk in pd.read_csv(input_file, chunksize=10000):

       input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
       input_processed = input_slice.where((pd.notnull(input_slice)), None)
       print("input processed")
       job = bulk.create_insert_job(target_obj, contentType='CSV')
       records = input_processed.to_dict('records')
       csv_iter = CsvDictsAdapter(iter(records))
       batch = bulk.post_batch(job, csv_iter)
       bulk.wait_for_batch(job, batch)
       bulk.close_job(job)
       print("Done. Gift Aid Declaration 10000 uploaded .")

print("Done All Gift Aid Declaration uploaded")

