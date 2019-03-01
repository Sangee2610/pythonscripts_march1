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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/prd_contact_reload_fail2.csv"

#Target SFDC Object name
target_obj = "Contact"

# Mapping of Input csv Fields to SalesForce Fields




sf_mapping = {
       'contactkey': 'Contact_Key__c',
       'accountkey': 'Account_Key__c',
       'account': 'AccountId',
       'title': 'Title',
       'salutation': 'Salutation',
       'first_name': 'FirstName',
       'last_name': 'LastName',
       'prefered_first_name': 'Prefered_First_Name__c',
       'branchkey': 'Branch_Key__c',
       'branch': 'Branch__c',
       'gender': 'Gender__c',
       'email': 'Email',
       'email_source_date': 'Email_Source_Date__c',
       'accountholdername': 'Account_Holder_Name__c',
       'bankname': 'Bank_Name__c',
       'accountnumber': 'Account_Number__c',
       'sortcode': 'Sort_Code__c',
       'phone_1': 'Phone_1__c',
       'phone_2': 'Phone_2__c',
       'phone_2_date': 'Phone_2_Date__c',
       'edd': 'EDD__c',
       'volunteer': 'Volunteer__c',
       'staff': 'Staff__c',
       'practitioner':'Practitioner__c',
       'mailingstreet': 'Mailingstreet',
       'mailingcity': 'Mailingcity',
       'mailingstate': 'Mailingstate',
       'mailingcountry': 'Mailingcountry',
       'mailingpostalcode': 'mailingpostalcode',
       'mailinglongitude': 'Mailing_Longtitude__c',
       'include': 'Include__c',
       'record_type_id':'RecordTypeId',
       'owner_':'OwnerId'
      
}


# Read the input file in batches and upload to SalesForce
for gm_chunk in pd.read_csv(input_file, chunksize=5000):

	input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
	input_processed = input_slice.where((pd.notnull(input_slice)), None)
       
	job = bulk.create_insert_job(target_obj, contentType='CSV')
	records = input_processed.to_dict('records')
	csv_iter = CsvDictsAdapter(iter(records))
	batch = bulk.post_batch(job, csv_iter)
	bulk.wait_for_batch(job, batch)
	bulk.close_job(job)
	print("Done. Accounts uploaded 5000.")
print("All Done")
