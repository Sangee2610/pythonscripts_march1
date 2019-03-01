import psycopg2

import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_staging_recurring_donations_id;
CREATE TABLE prd_staging_recurring_donations_id AS
SELECT 
(CASE WHEN First_Contact_Key__c = '' THEN NULL ELSE CAST(CAST(First_Contact_Key__c as FLOAT8) as INT) END) as ContactKey,
(CASE WHEN Recurring_Donations_Key__c = '' THEN NULL ELSE CAST(CAST(Recurring_Donations_Key__c as FLOAT8) as INT) END) as RecurringDonationsKey,
(CASE WHEN MandateKey__c = '' THEN NULL ELSE CAST(CAST(MandateKey__c as FLOAT8) as INT) END) as MandateKey,
(CASE WHEN Payment_profile_Key__c = '' THEN NULL ELSE CAST(CAST(Payment_profile_Key__c as FLOAT8) as INT) END) as PaymentprofileKey,
Id,
npe03__Contact__c as Contact,
npsp4hub__Mandate__c as Mandate,
npsp4hub__Payment_Profile__c as PaymentProfile,
(CASE WHEN npe03__Installment_Amount__c = '' THEN NULL ELSE CAST(npe03__Installment_Amount__c as FLOAT8) END) as InstallmentAmount,
(CASE WHEN npe03__Next_Payment_Date__c = '' THEN Renewal_Date__c ELSE npe03__Next_Payment_Date__c END) as NextDonationDate
from prd_landing_recurring_donations_id
""")

conn.commit()

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_Opportunity;
CREATE TABLE prd_Staging_Opportunity as
SELECT ROW_NUMBER () OVER (ORDER BY a.ContactKey) as OpportunityKey,
b.AccountKey as AccountKey,
a.ContactKey,
a.RecurringDonationsKey,
a.MandateKey,
a.PaymentprofileKey,
c.First_Name || c.Last_Name || 'Membership'||a.NextDonationDate as Name,
a.Id as Recurring_Donations,
b.accountid as Account,
a.Contact,
a.Mandate,
a.PaymentProfile,
a.InstallmentAmount as Amount,
(CASE WHEN a.NextDonationDate = '' THEN NULL 
ELSE TO_TIMESTAMP(a.NextDonationDate, 'YYYY-MM-DD') END) as CloseDate,
NULL as Type,
NULL as Description,
NULL as Source,
'Pledged' as Stage,
'0051v000005QT8qAAG' as Owner_,
'CARE' as datasource
FROM prd_staging_recurring_donations_id a
inner join 
(SELECT i.*, row_number() over (partition by contactkey order by accountkey desc) as seqn
FROM prd_staging_contactkey_id i) b
on a.ContactKey = b.ContactKey and seqn = 1
inner join
(SELECT First_Name, Last_Name, ContactKey, row_number() over (partition by contactkey order by accountkey desc) as seqnum
FROM prd_staging2_contact i) c 
on c.ContactKey = a.ContactKey and seqnum = 1;
""")

conn.commit()
conn.close()
