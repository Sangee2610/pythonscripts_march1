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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/prd_paymentprofile_bank_update.csv"

#Target SFDC Object name
target_obj = "cpm__Payment_Profile__c"

# Mapping of Input csv Fields to SalesForce Fields

sf_mapping = {
              'accountnumber': 'Account_Number__c',
              'banknumber': 'cpm__Bank_Account__c',
              'id': 'Id'
              }

# Read the input file in batches and upload to SalesForce
for gm_chunk in pd.read_csv(input_file, chunksize=5000):

       input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
       input_processed = input_slice.where((pd.notnull(input_slice)), None)
       print("input processed")
       job = bulk.create_update_job(target_obj, contentType='CSV')
       records = input_processed.to_dict('records')
       csv_iter = CsvDictsAdapter(iter(records))
       batch = bulk.post_batch(job, csv_iter)
       bulk.wait_for_batch(job, batch)
       bulk.close_job(job)
       print("Done. PaymentProfile updated 5000.")

print("Done All PaymentProfile updated")

