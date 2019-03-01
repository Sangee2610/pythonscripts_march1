import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv
import pandas as pd
import numpy as np

cur.execute("""
DROP TABLE IF EXISTS Landing_Contact;
CREATE TABLE Landing_Contact(
    ContactKey text,
    AccountKey text,
    Account text,
    Title text,
    Salutation text,
    First_Name text,
    Last_Name text,
    Prefered_First_Name text,
    Branch text,
    HomePSAOfficeKey text,
    SecondHomePSAOfficeKey text,
    Gender text,
    Email text,
    Email_Source_Date text,
    Volunteer text,
    Staff text,
    Practitioner text,
    Phone_1 text,
    Phone_1_Date text,
    Phone_2 text,
    Phone_2_Date text,
    EDD text,
    Source text,
    Source_Date text,
    Source_Description text,
    mailingStreet text,
    mailingCity text,
    mailingState text,
    mailingCountry text,
    mailingPostalCode text,
    mailingLatitude text,
    mailinglongtitdude text,
    include text,
    Owner_ text
)
""")

input_file = '/home/baadmin/NCT_ETL/input_files/Contact.csv'

def data_cleaning_loading(filename):
    new_filename = filename.replace(".csv", "_corrected.csv")
    f = open(filename, encoding="ISO-8859-1")
    g = open(new_filename, "w+", encoding="utf-8")
    new_rows = []
    changes = { ',' : ''}

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
    cur.copy_from(g, 'Landing_Contact',  sep=",")
    conn.commit()
    g.close()
    f.close()


data_cleaning_loading(input_file)
