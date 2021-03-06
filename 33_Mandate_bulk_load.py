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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/prd_staging_mandate_extract.csv"

#Target SFDC Object name
target_obj = "cpm__Mandate__c"

# Mapping of Input csv Fields to SalesForce Fields

sf_mapping = {'contactkey': 'Contact_Key__c',
              'paymentprofilekey': 'Payment_Profile_Key__c',
              'mandatekey': 'Mandate_Key__c',
              'paymentprofile': 'cpm__Payment_Profile__c',
              'type': 'cpm__Type__c',
              'target': 'cpm__Target__c',
              'active': 'cpm__Active__c',
              'sourcecode':'Source_Code__c',
              'ddreference': 'DD_Reference__c',
              'owner_': 'OwnerId',
              'bookingkey': 'BookingKey__c',
              'directdebitkey': 'DirectDebitKey__c',
              'datasource': 'Data_Source__c'
              }

# Read the input file in batches and upload to SalesForce
for gm_chunk in pd.read_csv(input_file, chunksize=200):

       input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
       input_processed = input_slice.where((pd.notnull(input_slice)), None)
       print("input processed")
       job = bulk.create_insert_job(target_obj, contentType='CSV')
       records = input_processed.to_dict('records')
       csv_iter = CsvDictsAdapter(iter(records))
       batch = bulk.post_batch(job, csv_iter)
       bulk.wait_for_batch(job, batch)
       bulk.close_job(job)
       print("Done. Mandate uploaded 200.")
       break
print("Done All Mandate uploaded")

