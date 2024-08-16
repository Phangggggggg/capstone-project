{{
    config(
        materialized="view"
    )
}}

SELECT 
    count(distinct caseNumber) as case_cnt,
    count(distinct case when receivedDate = curr_date then caseNumber end) as today_case_cnt,
    count(distinct case when receivedDate = date_sub(day, 1,curr_date) then caseNumber end) as yesterday_case_cnt,
    count(distinct case when receivedDate = curr_date then caseNumber end)  - count(distinct case when receivedDate = date_sub(day, 1,curr_date) then caseNumber end) as case_change,
    avg(wageRangeFrom) as avg_wage
FROM 
    'visa' t1
LEFT JOIN 
    (SELECT 
        max(receivedDate) as curr_date
    FROM 
    'visa'
    ) t2 on 1=1
WHERE 
    receivedDate >= '2023-01-01' 