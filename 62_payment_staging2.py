import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv

# To Do: Need to change Join table name and field name from sales force look up script(Account,Installment,
#  Payment profile, Contact and Mandate)

cur.execute("""
drop table if exists prd_Staging2_Payment;
CREATE TABLE prd_Staging2_Payment as
SELECT 
    SP.PaymentKey,    
    SCI.Accountkey,
    SP.ContactKey,
    SP.PaymentPlanKey as InstallmentKey,
    SMI.MandateKey,
    SMI.PaymentProfile_Key as PaymentProfileKey ,
    SII.id as Installment,
    SMI.PaymentProfile_id as PaymentProfile,    
    SCI.AccountId as Account,
    SCI.Id as Contact,
    SMI.Id as Mandate,
    SP.Description,
    SP.ReversalReasonCode,
    SP.PaymentMethod,
    SP.PaymentProcessor,
    SP.Target,
    SP.TransactionDate as CollectionDate,
    SP.PaymentReference,
    SP.Amount,
    SP.Source,    
    SP.Include,    
    '0051v000005QT8qAAG' as Owner,
    SP.Status,
    SP.ProductGroup,
    SP.EligibeforGiftAid,
    SP.GiftAidClaimed,
    SP.TransactionType,
    'CARE' as datasource
    FROM prd_Staging1_Payment SP
        left outer join
        prd_Staging_Installment_Id SII
        on
        SP.PaymentPlanKey = SII.InstallmentKey
        left outer join
        (select *, row_number() over (partition by contactkey order by contactkey desc) as seqnum
        from
        prd_staging_contactkey_id) SCI
        on
        SP.ContactKey = SCI.ContactKey and seqnum =1
        left outer join
        (select *, row_number() over (partition by contactkey order by contactkey desc) as seqnum2
        from 
        prd_staging_mandate_id) SMI
        on
        SP.ContactKey = SMI.ContactKey and seqnum2 =1;
""")

conn.commit()
conn.close()
