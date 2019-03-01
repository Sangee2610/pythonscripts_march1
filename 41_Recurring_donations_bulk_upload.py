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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/prd_recurring_donations_failed.csv"
#input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/prd_staging2_recurringdonations.csv"

#Target SFDC Object name
target_obj = "npe03__Recurring_Donation__c"

# Mapping of Input csv Fields to SalesForce Fields

sf_mapping = {
       'firstcontactkey' : 'First_Contact_Key__c',
       'secondcontactkey' : 'Second_Contact_Key__c',
       'paymentprofilekey': 'Payment_profile_Key__c',
       'mandatekey' : 'MandateKey__c',
       'recurringdonationskey' : 'Recurring_Donations_Key__c',
       'name' : 'Name',
       'first_contact' : 'npe03__Contact__c',
       'second_contact' : 'Second_Contact__c',
       'membership_type' : 'Membership_Type__c',
       'membership_type_code':'Membership_Type_Code__c',
       'membershipperiod' : 'Membership_Period__c',
       'date_established' : 'npe03__Date_Established__c',
       'renewal_date' : 'Renewal_Date__c',
       'payment_method' : 'npsp4hub__Payment_Method__c',
       'mandate' : 'npsp4hub__Mandate__c',
       'payment_profile' : 'npsp4hub__Payment_Profile__c',
       'source' : 'Source__c',
       'owner_' : 'OwnerId',
       'direct_debit_key' : 'Direct_Debit_Key__c',
       'care_payment_frequency': 'CARE_Payment_Frequency__c',
       'source_code' : 'Source_Code__c',
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
       print("Done. Recurring uploaded 200.")
       break

print("Done All Recurring uploaded")

