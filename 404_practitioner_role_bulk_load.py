import json
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
from time import sleep
from salesforce_bulk import CsvDictsAdapter
import pandas as pd
import config as cfg

bulk = SalesforceBulk(username=cfg.USERNAME, 
	password=cfg.PASSWORD, 
	security_token=cfg.SECURITY_KEY, sandbox=True)

#Source CSV File path for Account
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/prd_Staging_Practitioner_role_extract.csv"

#Target SFDC Object name
target_obj = "Practitioner_Role__c"

# Mapping of Input csv Fields to SalesForce Fields
sf_mapping = {  
        'name': 'Name__c', 	
        'contactkey': 'ContactKey__c',
        'contact': 'Contact__c',
        'role': 'Role__c',	
        'acquistiondate': 'Acquisition_Date__c',
        'expirydate': 'Expiry_Date__c',
        'active': 'Active__c',
        'maximumcouple': 'Maximum_Couple__c',
        'maximumpeople': 'Maximum_People__c',
        'owner_': 'Owner__c',
        'datasource': 'Data_Source__c'
          }


# Read the input file in batches and upload to SalesForce
for gm_chunk in pd.read_csv(input_file, chunksize=200, encoding="ISO-8859-1"):
       input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
       input_processed = input_slice.where((pd.notnull(input_slice)), None)
       print("input processed")
       job = bulk.create_insert_job(target_obj, contentType='CSV')
       records = input_processed.to_dict('records')
       csv_iter = CsvDictsAdapter(iter(records))
       batch = bulk.post_batch(job, csv_iter)
       bulk.wait_for_batch(job, batch)
       bulk.close_job(job)
       print("Done. Accounts uploaded 200.")
       break

print("Done All accounts uploaded")
