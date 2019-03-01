import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv
import pandas as pd
import numpy as np


cur.execute("""DROP TABLE IF EXISTS prd_Staging1_Installment;
CREATE TABLE prd_Staging1_Installment as
SELECT (CASE WHEN membershipkey = '' THEN NULL ELSE CAST(membershipkey as INT) END) as membershipkey,
(CASE WHEN BookingKey = '' THEN NULL ELSE CAST(BookingKey as INT) END) as BookingKey,
(CASE WHEN DirectDebitKey = '' THEN NULL ELSE CAST(DirectDebitKey as INT) END) as DirectDebitKey,
(CASE WHEN PaymentPlanKey = '' THEN NULL ELSE CAST(PaymentPlanKey as INT) END) as PaymentPlanKey,
(CASE WHEN ScheduledPaymentNumber = '' THEN NULL ELSE CAST(ScheduledPaymentNumber as INT) END) as ScheduledPaymentNumber,
(CASE WHEN CourseBooking = '' THEN NULL ELSE CAST(CourseBooking as INT) END) as CourseBooking,
(CASE WHEN Membership = '' THEN NULL ELSE CAST(Membership as INT) END) as Membership,
ParentRecordIdentifier,
(CASE WHEN DueDate = '' THEN NULL ELSE TO_TIMESTAMP(DueDate, 'YYYY-MM-DD HH24:MI:SS') END) as DueDate,
(CASE WHEN ClaimDate = '' THEN NULL ELSE TO_TIMESTAMP(ClaimDate, 'YYYY-MM-DD HH24:MI:SS') END) as ClaimDate,
(CASE WHEN AmountDue = '' THEN NULL ELSE CAST(CAST(AmountDue as FLOAT) as INT) END) as AmountDue,
(CASE WHEN AmountOutstanding = '' THEN NULL ELSE CAST(CAST(AmountOutstanding as FLOAT) as INT) END) as AmountOutstanding,
(CASE WHEN DirectDebit = '' THEN NULL ELSE CAST(DirectDebit as INT) END) as DirectDebit,
'0051v000005QT8qAAG' as Owner_,
'CARE' as datasource
FROM prd_Landing_Installment""")

conn.commit()

cur.execute("""DROP TABLE IF EXISTS prd_Staging2_Installment;
CREATE TABLE prd_Staging2_Installment as
SELECT null as Accountkey,
SO.ContactKey,
SI.Membership as RecurringDonationskey,
SO.paymentprofilekey as PaymentProfileKey,
SO.mandatekey as Mandatekey,
SG.giftaiddeclarationkey,
SOI.opportunity_key,
PaymentPlanKey as InstallmentKey,
SI.ScheduledPaymentNumber,
NULL as Account,
SO.contact,
SO.id as recurringdonation,
SO.PaymentProfile,
SO.Mandate,
SG.Id as GiftAidDeclaration,
SOI.Id as Opportunity,
SI.ParentRecordIdentifier,
SI.AmountDue as Amount,
SI.AmountDue as Amount_open,
SI.DueDate,
SI.ClaimDate,
NULL as Status,
SI.AmountOutstanding,
'0051v000005QT8qAAG' as Owner_,
SI.CourseBooking as CourseBookingKey,
'CARE' as datasource
FROM prd_Staging1_Installment SI
LEFT OUTER JOIN
(select *, row_number() over (partition by RecurringDonationsKey order by contactkey desc) as seqnum
from
prd_staging_recurring_donations_id) SO 
on SO.RecurringDonationsKey = SI.membershipkey and seqnum = 1
LEFT OUTER JOIN 
(select *, row_number() over (partition by contactkey order by contactkey desc) as seqnum2
from prd_staging_giftaid_id where contactkey > 0) SG on SG.contactkey = SO.contactkey and seqnum2 = 1
LEFT OUTER JOIN  prd_staging_opportunity_id SOI on SOI.recurring_donations_key = SO.RecurringDonationsKey 
                                      and SOI.paymentprofile_key = SO.paymentprofilekey 
                                      and SOI.contactkey = SO.ContactKey
""")
conn.commit()
conn.close()
