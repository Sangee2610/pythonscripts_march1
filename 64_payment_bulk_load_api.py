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
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/staging2_payment.csv"

#Target SFDC Object name
target_obj = "cpm__Payment__c"

# Mapping of Input csv Fields to SalesForce Fields


sf_mapping = {
    'paymentkey': 'Payment_Key__c',
    'accountkey': 'Account_key__c',
    'contactkey': 'Contact_Key__c',
    'installmentkey': 'Installment_Key__c',
    'mandatekey': 'Mandate_Key__c',
    'paymentprofilekey': 'Payment_Profile_Key__c',
    'installment': 'cpm__Installment__c',
    'paymentprofile': 'cpm__Payment_Profile__c',
    'account': 'cpm__Account__c',
    'contact': 'cpm__Contact__c',
    'mandate': 'cpm__Mandate__c',
    'description': 'cpm__Description__c',
    'reversalreasoncode':'cpm__Reversal_Reason_Code__c',
    'paymentmethod': 'cpm__Payment_Method__c',
    'paymentprocessor':'cpm__Payment_Processor__c',
    'target': 'cpm__Target__c',
    'collectiondate': 'cpm__Collection_Date__c',
    'paymentreference': 'cpm__Payment_Reference__c',
    'amount': 'cpm__Amount__c',
    'owner': 'OwnerId',
    'status': 'Status__c',
    'productgroup': 'Product_Group__c',
    'eligibeforgiftaid': 'Eligible_for_Gift_Aid__c',
    'giftaidclaimed': 'Gift_Aid_Claimed__c',
    'transactiontype': 'Transaction_Type__c',
    'datasource': 'Data_Source__c'
}


# Read the input file in batches and upload to SalesForce
counter = 0
for gm_chunk in pd.read_csv(input_file, chunksize=200):

    if counter < 28:
        counter = counter + 1
        continue
    else:
        input_slice = gm_chunk[list(sf_mapping.keys())].rename(columns=sf_mapping)
        input_processed = input_slice.where((pd.notnull(input_slice)), None)
        job = bulk.create_insert_job(target_obj, contentType='CSV')
        records = input_processed.to_dict('records')
        csv_iter = CsvDictsAdapter(iter(records))
        batch = bulk.post_batch(job, csv_iter)
        bulk.wait_for_batch(job, batch)
        bulk.close_job(job)
        print("Done. Payment uploaded 200.")
        break

