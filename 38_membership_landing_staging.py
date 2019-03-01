

import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT
cur = conn.cursor()

#'Membership' || Membership_Type AS Name,
#MembershipPeriod || ' ' || 'Membership' || ' ' || Installment_Period || ' ' || Payment_Method AS Name,

cur.execute("""
DROP TABLE IF EXISTS prd_Staging_RecurringDonations;
CREATE TABLE prd_Staging_RecurringDonations as
SELECT (CASE WHEN ContactKey = '' THEN NULL ELSE CAST(ContactKey as INT) END) as ContactKey,
(CASE WHEN DirectDebitKey = '' THEN NULL ELSE CAST(DirectDebitKey as INT) END) as Direct_Debit_Key,
NULL as MandateKey,
NULL as PaymentprofileKey,
(CASE WHEN Membership_Key = '' THEN NULL ELSE CAST(Membership_Key as INT) END) as RecurringDonationsKey,
Membership_Term ||' Months' || ' ' || 'Membership' || ' ' || null || ' ' || Payment_Method AS Name,
NULL as MembershipNumber,
Null as First_Contact,
Null as Second_Contact,
(CASE WHEN First_Contact = '' THEN NULL ELSE CAST(First_Contact as INT) END) as FirstContactKey,
(CASE WHEN Second_Contact = '' THEN NULL ELSE CAST(Second_Contact as INT) END) as SecondContactKey,
NULL as Origin,
Membership_Type as Membership_Type_code,
(CASE WHEN Membership_Type = 'AI' THEN '1 year standard - Individual'
WHEN Membership_Type = 'AJ' THEN '1 year standard - Joint'
WHEN Membership_Type = 'EI' THEN '18 month - Individual'
WHEN Membership_Type = 'EJ' THEN '18 month - Joint'
WHEN Membership_Type = 'FI' THEN '4 year - Individual'
WHEN Membership_Type = 'FJ' THEN '4 year - Joint'
WHEN Membership_Type = 'LIFE' THEN 'Life'
WHEN Membership_Type = 'LIFJ' THEN 'Life Joint Membership'
WHEN Membership_Type = 'RI' THEN 'Reduced rate 1 year - Ind'
WHEN Membership_Type = 'RJ' THEN 'Reduced rate 1 year - Joint'
WHEN Membership_Type = 'SI' THEN 'Staff Membership - Individual'
WHEN Membership_Type = 'SJ' THEN 'Staff Membership - Joint'
WHEN Membership_Type = 'TI' THEN 'Ten Year - Individual'
WHEN Membership_Type = 'TJ' THEN 'Ten Year - Joint'
WHEN Membership_Type = 'VI' THEN '1 year volunteer/practitioner - Ind'
WHEN Membership_Type = 'VJ' THEN '1 year volunteer/practitioner - Jnt'
WHEN Membership_Type = 'WI' THEN '50% 1 year standard - Indv'
WHEN Membership_Type = 'WJ' THEN '50% 1 year standard - Joint'
ELSE Membership_Type END) as Membership_Type,
Joined as Date_Established,
Cancelation_Date,
Cancelation_Reason,
Canceled_By,
Membership_Term ||' Months' as MembershipPeriod,
Renewal_Date,
(CASE WHEN Payment_Method = 'DD' THEN 'Direct Debit'
WHEN Payment_Method = 'CC' THEN 'CreditCard'
WHEN Payment_Method = 'SO' THEN 'Standing Order'
ELSE Payment_Method END) as Payment_Method,
Payment_Frequency as CARE_Payment_Frequency,
NULL as Payment_Processor,
NULL as Target,
NULL as Installment_Period,
NULL as Amount,
NULL as Installments,
NULL as Schedule_Type,
NULL as Mandate,
NULL as Payment_Profile,
Source as Source_code,
Source_Description as Source,
Include,
'CARE' as datasource,
'0051v000005QT8qAAG'as Owner_
FROM prd_Landing_Membership;
""")

conn.commit()
conn.close()
