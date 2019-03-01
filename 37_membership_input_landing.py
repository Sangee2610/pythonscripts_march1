import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS prd_Landing_Membership;
CREATE TABLE prd_Landing_Membership(
    ContactKey text,
    DirectDebitKey text,
    Membership_Key text,
    First_Contact text,
    Second_Contact text,
    Membership_Type text,
    Joined text,
    Cancelation_Date text,
    Cancelation_Reason text,
    Canceled_By text,
    Membership_Term text,
    Renewal_Date text,
    Payment_Method text,
    Payment_Frequency text,
    Direct_Debit text,
    Source text,
    Source_Description text,
    Include text,
    Owner_ text
)
""")

memsample = '/home/baadmin/NCT_ETL/input_files/Membership.csv'

def data_cleaning_loading(filename):
    new_filename = filename.replace(".csv", "_corrected.csv")
    f = open(filename, encoding="ISO-8859-1")
    g = open(new_filename, "w+",encoding="ISO-8859-1")
    new_rows = []
    changes = { ',' : '',}

    for row in csv.reader(f, quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL, skipinitialspace=True):     # iterate over the rows in the file
        new_row = row      # at first, just copy the row
        for key, value in changes.items(): # iterate over 'changes' dictionary
            new_row = [ x.replace(key, value) for x in new_row ] # make the substitutions
        new_rows.append(new_row) # add the modified rows
    new_rows = new_rows[1:] #Remove header
    for new_row in new_rows:
        g.write(str(",".join(new_row)) + "\n")
    g.close()
    g = open(new_filename)
    cur.copy_from(g, 'prd_Landing_Membership',  sep=",")
    conn.commit()
    g.close()
    f.close()

data_cleaning_loading(memsample)
