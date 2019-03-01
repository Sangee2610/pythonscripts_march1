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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/Opportunity_staging_extract.csv"

#Target SFDC Object name
target_obj = "Opportunity"

# Mapping of Input csv Fields to SalesForce Fields
sf_mapping = {'opportunitykey': 'Opportunity_Key__c',
              'accountkey': 'Account_Key__c',
              'contactkey': 'Contact_Key__c',
              'recurringdonationskey': 'Recurring_Donations_Key__c',
              'mandatekey': 'Mandate_Key__c',
              'paymentprofilekey': 'Payment_Profile_Key__c',
              'recurring_donations': 'npe03__Recurring_Donation__c',
              'name': 'Name',
              'account': 'AccountId',
              'contact': 'Contact__c',
              'mandate': 'npsp4hub__Mandate__c',
              'paymentprofile': 'npsp4hub__Payment_Profile__c',
              'closedate': 'CloseDate',
              'type': 'Type',
              'description': 'Description',
              'source': 'Source__c',
              'stage':'StageName',
              'owner_': 'OwnerId',
              'datasource': 'Data_Source__c'
              }


# Read the input file in batches and upload to SalesForce
for gm_chunk in pd.read_csv(input_file, chunksize=10000, encoding="ISO-8859-1"):

       input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
       input_processed = input_slice.where((pd.notnull(input_slice)), None)
       print("input processed")
       job = bulk.create_insert_job(target_obj, contentType='CSV')
       records = input_processed.to_dict('records')
       csv_iter = CsvDictsAdapter(iter(records))
       batch = bulk.post_batch(job, csv_iter)
       bulk.wait_for_batch(job, batch)
       bulk.close_job(job)
       print("Done. Accounts uploaded 10000.")

print("Done All accounts uploaded")
