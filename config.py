import psycopg2
DATABASE_CONNECT = psycopg2.connect("host=localhost port=5432 dbname=nctdb user=postgres password=Password!234")


#Authentication of etl
USERNAME = 'nctcaremigration@nct.co.uk.etl'
PASSWORD = 'etlimport2'
SECURITY_KEY ='4VTonXuTqvh86eUlKPcGdJ51'
SANDBOX= True


'''
#Authentication of uat
USERNAME = 'nctcaremigration@nct.co.uk.uat'
PASSWORD = 'etlimport1'
SECURITY_KEY ='UDYQv9Mnh8SCWISs397yZGGP'
SANDBOX= True
'''
