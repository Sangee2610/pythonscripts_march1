import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
import csv
import pandas as pd
import numpy as np
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_Landing_Paymentprofile_Id;
CREATE TABLE prd_Landing_Paymentprofile_Id (
	index text,
directdebitkey text,
ContactKey text,
Contact text,
PaymentProfileKey text,
Id text
    
)
""")

#index text,
#directdebitkey text,
#ContactKey text,
#Contact text,
#PaymentProfileKey text,
#Id text

accsample = '/home/baadmin/NCT_ETL/input_files/pg_extract_prd/PaymentProfileId_sf.csv'


def data_cleaning_loading(filename):
    new_filename = filename.replace(".csv", "_corrected.csv")
    f = open(filename, encoding="utf-8")
    g = open(new_filename, "w+", encoding="ISO-8859-1")
    new_rows = []
    changes = {',': ''}

    for row in csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL,
                          skipinitialspace=True):  # iterate over the rows in the file
        new_row = row  # at first, just copy the row
        for key, value in changes.items():  # iterate over 'changes' dictionary
            new_row = [x.replace(key, value) for x in new_row]  # make the substitutions
        new_rows.append(new_row)  # add the modified rows
    new_rows = new_rows[1:]  # Remove header
    for new_row in new_rows:
        g.write(str(",".join(new_row)) + "\n")
    g.close()
    g = open(new_filename)
    cur.copy_from(g, 'prd_Landing_Paymentprofile_Id', sep=",")
    conn.commit()
    g.close()
    f.close()


data_cleaning_loading(accsample)
