{{
    config(
        materialized="view"
    )
}}


-- Show location distribution of submitted cases
SELECT
    worksiteState,
    count(1) AS records_cnt,
    countDistinct(caseNumber) AS case_cnt,
    avg(wageRangeFrom) AS avg_wage
FROM     
    'visa'
WHERE 
    receivedDate >= '2023-01-01'
GROUP BY 1