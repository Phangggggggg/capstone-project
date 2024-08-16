{{
    config(
        materialized="view"
    )
}}

SELECT 
    receivedDate as received_date,
    count(1) as records_cnt,
    count(distinct caseNumber) as case_cnt
FROM 
    "visa"
WHERE 
    receivedDate >= '2023-01-01'
GROUP BY 1
ORDER BY 1 