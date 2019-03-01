import psycopg2
import csv
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()
cur.execute("""
DROP TABLE IF EXISTS prd_landing_recurring_donations_id;
CREATE TABLE prd_landing_recurring_donations_id(
First_Contact_Key__c text,
Recurring_Donations_Key__c text,
MandateKey__c text,
Payment_profile_Key__c text,
Id text,
npe03__Contact__c text,
npsp4hub__Mandate__c text,
npsp4hub__Payment_Profile__c text,
npe03__Installment_Amount__c text,
npe03__Next_Payment_Date__c text,
Renewal_Date__c text
)
""")

ddsample = '/home/baadmin/NCT_ETL/input_files/pg_extract_prd/recurring_donations_id_sf.csv'


def data_cleaning_loading(filename):
    new_filename = filename.replace(".csv", "_corrected.csv")
    f = open(filename, encoding="ISO-8859-1")
    g = open(new_filename, "w+", encoding="ISO-8859-1")
    new_rows = []
    changes = {',': '', }

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
    cur.copy_from(g, 'prd_landing_recurring_donations_id', sep=",")
    conn.commit()
    g.close()
    f.close()


data_cleaning_loading(ddsample)

conn.close()
