	
{{
    config(
        materialized="view"
    )
}}



SELECT 
    *
    , records *1.0 / sum(records) over () as records_pcent
    , cases *1.0 / sum(cases) over () as cases_pcent
FROM
    (SELECT 
        *
    FROM
        (SELECT
            positionName,
            first_value(jobTitle) over (partition by positionName order by case_cnt desc) as most_common_title,
            first_value(avg_wage) over (partition by positionName order by case_cnt desc) as most_common_title_wage,
            sum(records_cnt) over (partition by positionName) as records,
            sum(case_cnt) over (partition by positionName) as cases
        FROM
            (SELECT 
                cast(left(socCode,2) as int) as socCode,
                jobTitle, 
                count(1) as records_cnt,
                count(distinct caseNumber) as case_cnt,
                avg(wageRangeFrom) as avg_wage
            FROM 
                'visa'
            WHERE 
                receivedDate >= '2023-01-01'
            GROUP BY 1,2
            ) t1
        LEFT JOIN 
            (SELECT 
                socCode, 
                positionName
            FROM 
                'soc_code'
            ) t2 on t1.socCode = t2.socCode
        )
    GROUP BY 1,2,3,4,5
    ORDER BY 4 DESC
    )