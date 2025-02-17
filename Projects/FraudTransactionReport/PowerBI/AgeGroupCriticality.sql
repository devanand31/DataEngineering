--Pie chart showing Age Group criticality trend
SELECT c.AGE_GROUP,
       COUNT(*) AS HIGH_CRITICAL_ALERT_COUNT
from CURATED.CREDIT_TRANSACTIONS tr
    LEFT JOIN CURATED.CUSTOMER c
        ON (tr.CUSTOMER_ID = c.CUSTOMER_ID)
WHERE FRAUD_CRITICALITY = 'HIGH'
GROUP BY c.AGE_GROUP;