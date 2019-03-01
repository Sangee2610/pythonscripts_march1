import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
import csv

cur.execute("""
DROP TABLE IF EXISTS Landing_PSA_Office;
CREATE TABLE Landing_PSA_Office(
  Name text,
  PSAOfficeKey text,
  Reference text,
  Cash text,
  CapacityPSA text,
  CapacityPSAEmail text,
  EnquiryPSA text,
  EnquiryPSAEmail text,
  Datasource text,
  Owner_ text
)
""")

input_file = '/home/baadmin/NCT_ETL/input_files/psa_office.csv'

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
    cur.copy_from(g, 'Landing_PSA_Office',  sep=",")
    conn.commit()
    g.close()
    f.close()

data_cleaning_loading(input_file)
