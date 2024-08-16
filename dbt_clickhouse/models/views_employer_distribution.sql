{{
    config(
        materialized="view"
    )
}}

SELECT 
    splitByChar( ',', upper(employerName))[1] as company_name,
    count(1) as records_cnt,
    count(distinct caseNumber) as case_cnt,
    avg(wageRangeFrom) as avg_wage
FROM 
    'visa'
WHERE 
    receivedDate >= '2023-01-01' 
GROUP BY 1
