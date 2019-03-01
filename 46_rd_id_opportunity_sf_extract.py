# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 16:53:28 2018

@author: css113429
"""

import json
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
from time import sleep
from salesforce_bulk import CsvDictsAdapter
import unicodecsv
import csv
import pandas as pd
import numpy as np
import config as cfg

##Salesforce table extraction
bulk = SalesforceBulk(username=cfg.USERNAME, 
	password=cfg.PASSWORD, 
	security_token=cfg.SECURITY_KEY, sandbox=True)

#Source CSV File path for Account
input_file = "/home/baadmin/NCT_ETL/input_files/pg_extract_prd/recurring_donations_id_sf.csv"

sf_fields = ['First_Contact_Key__c','Recurring_Donations_Key__c', 'MandateKey__c',
             'Payment_profile_Key__c', 'Id', 'npe03__Contact__c', 'npsp4hub__Mandate__c',
             'npsp4hub__Payment_Profile__c', 'npe03__Installment_Amount__c',
             'npe03__Next_Payment_Date__c', 'Renewal_Date__c']
             
#Target SFDC Object name
target_obj = "npe03__Recurring_Donation__c"

# Extract the data from salesforce and save it to csv

job = bulk.create_query_job(target_obj, contentType='CSV')
sql = "SELECT " + ",".join(sf_fields) + " FROM " + target_obj
batch = bulk.query(job, sql)
bulk.close_job(job)
while not bulk.is_batch_done(batch):
    sleep(10)

for result in bulk.get_all_results_for_query_batch(batch):
    reader = unicodecsv.DictReader(result, encoding='utf-8')
    with open(input_file, 'a') as f:
    	pd.DataFrame(list(reader)).to_csv(f, header=True,index=False)
