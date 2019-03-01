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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/staging_installment_extract.csv"

#Target SFDC Object name
target_obj = "cpm__Installment__c"

# Mapping of Input csv Fields to SalesForce Fields

sf_mapping = {'contactkey': 'Contact_Key__c',
              'recurringdonationskey': 'Recurring_Donations_Key__c',
              'paymentprofilekey': 'Payment_Profile_Key__c',
              'mandatekey':'Mandate_Key__c',
              'giftaiddeclarationkey': 'GiftAid_Declaration_Key__c',
              'opportunity_key': 'Opportunity_Key__c',
              'installmentkey': 'Installment_Key__c',
              'scheduledpaymentnumber': 'Scheduled_Payment_Number__c',
              'contact': 'cpm__Contact__c',
              'recurringdonation': 'Recurring_Donations__c',
              'paymentprofile': 'cpm__Payment_Profile__c',
              'mandate': 'cpm__Mandate__c',
              'giftaiddeclaration':'gaid__Gift_Aid_Declaration__c',
              'opportunity':'npsp4hub__Opportunity__c',
              'parentrecordidentifier':'Parent_Record_Identifier__c',
              'amount':'cpm__Amount__c',
              'amount_open': 'cpm__Amount_Open__c',
              'duedate':'cpm__Due_Date__c',
              'claimdate':'Claim_Date__c',
              'status':'cpm__Status__c',
              'amountoutstanding':'Amount_Outstanding__c',
              'owner_':'OwnerId',
              'coursebookingkey':'Course_Booking_key__c',
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
       print("Done. installment uploaded 10000 .")

print("Done All installment uploaded")

