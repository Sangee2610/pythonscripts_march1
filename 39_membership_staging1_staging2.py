import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS prd_Staging2_RecurringDonations;
CREATE TABLE prd_Staging2_RecurringDonations as
SELECT r.contactkey, direct_debit_key, pm.mandatekey, pp.paymentprofilekey, 
       recurringdonationskey, name, membershipnumber, SCI.id as first_contact, 
       SCC.id as second_contact, firstcontactkey, secondcontactkey, origin, Membership_Type_code, membership_type, 
       date_established, cancelation_date, cancelation_reason, canceled_by, 
       membershipperiod, renewal_date, payment_method, care_payment_frequency, 
       payment_processor, target, installment_period, amount, installments, 
       schedule_type, pm.id as mandate, pp.id as payment_profile, source_code, source, 
       include, owner_ ,'CARE' as datasource
  FROM prd_staging_recurringdonations r
	INNER JOIN 
	(SELECT i.*, row_number() over (partition by contactkey order by accountkey desc) as seqnum
	 FROM prd_staging_contactkey_id i) SCI 
	 on 
	SCI.ContactKey = r.firstcontactkey
	and seqnum = 1
	left outer join
	(SELECT i.*, row_number() over (partition by contactkey order by accountkey desc) as seqn
	 FROM prd_staging_contactkey_id i) SCC 
	 on 
	SCC.ContactKey = r.secondcontactkey
	and seqn = 1
	left outer join
	prd_staging_paymentprofile_id pp
	on
	pp.directdebitkey = r.direct_debit_key and
	pp.contactkey = r.contactkey
	left outer join
	prd_staging_mandate_id pm
	on
	pm.directdebitkey = r.direct_debit_key and
	pm.contactkey = r.contactkey;
""")

conn.commit()
conn.close()
