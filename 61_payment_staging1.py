import psycopg2
import config as cfg
conn = cfg.DATABASE_CONNECT

cur = conn.cursor()
import csv

cur.execute("""DROP TABLE IF EXISTS prd_Staging1_Payment;
CREATE TABLE prd_Staging1_Payment as
SELECT
ROW_NUMBER () OVER (ORDER BY Contact_Key) as PaymentKey,
NULL as Accountkey,
NULL as MandateKey,
NULL as PaymentProfileKey,
(CASE WHEN Contact_Key = '' THEN NULL ELSE CAST(Contact_Key as INT) END) as ContactKey,
(CASE WHEN Payment_Plan_Key = '' THEN NULL ELSE CAST(CAST(Payment_Plan_Key as FLOAT) as INT) END) as PaymentPlanKey,
(CASE WHEN Payment_Plan = '' THEN NULL ELSE CAST(CAST(Payment_Plan as FLOAT) as INT) END) as PaymentPlan,
Transaction_Type as TransactionType,
Contact,
(CASE WHEN Transaction_Date = '' THEN NULL ELSE TO_CHAR(TO_TIMESTAMP(Transaction_Date, 
'YYYY-MM-DD HH24:MI:SS') at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) as TransactionDate,
(CASE WHEN Amount = '' THEN NULL ELSE CAST(Amount as float) END) as Amount,
payment_method as Paymentmethod,
Status,
Source,
Product_Group as ProductGroup,
Eligibe_for_Gift_Aid as EligibeforGiftAid,
(CASE WHEN Gift_Aid_Claimed = '' THEN NULL ELSE CAST(Gift_Aid_Claimed as float) END) as GiftAidClaimed,
Include,
NULL as Description,
NULL as ReversalReasonCode,
NULL as PaymentProcessor,
NULL as Target,
NULL as PaymentReference,
'0051v000005QT8qAAG' as Owner,
'CARE' as datasource
FROM prd_Landing_Payment;
""")

conn.commit()
conn.close()
