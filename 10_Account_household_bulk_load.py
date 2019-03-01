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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/staging_account_household.csv"

#Target SFDC Object name
target_obj = "Account"

# Mapping of Input csv Fields to SalesForce Fields



sf_mapping = {'accountnumber': 'AccountNumber',
              'accountname': 'Name',
              'accountkey': 'account_Key__c',
              'branchcode': 'Branch_Code__c',
              'branchkey': 'Branch_Key__c',
              'billingstreet': 'Mailing_Street__c',
              'billingcity': 'Mailing_City__c',
              'billingstate': 'Mailing_State__c',
              'billingcountry': 'Mailing_Country__c',
              'billingpostcode': 'Mailing_Postal_Code__c',
              'billinglatitude': 'Mailing_Latitude__c',
              'billinglongitude': 'Mailing_Longtitude__c',
              'psaoffice': 'PSA_Area_Code__c',
              'mobilephone': 'Mobile__c',
              'description': 'description',
              'phone': 'phone',
              'record_type_id': 'RecordTypeId',
              'owner_': 'OwnerId',
              'branch': 'Branch__c',
              'datasource': 'Data_Source__c'

              }


# Read the input file in batches and upload to SalesForce
for gm_chunk in pd.read_csv(input_file, chunksize=200, encoding="ISO-8859-1"): #200-10000

       input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
       input_processed = input_slice.where((pd.notnull(input_slice)), None)

       job = bulk.create_insert_job(target_obj, contentType='CSV')
       records = input_processed.to_dict('records')
       csv_iter = CsvDictsAdapter(iter(records))
       batch = bulk.post_batch(job, csv_iter)
       bulk.wait_for_batch(job, batch)
       bulk.close_job(job)
       print("Done. Accounts uploaded 200.") #200->10000
       break  #ishi

print("Done. All Accounts uploaded")
