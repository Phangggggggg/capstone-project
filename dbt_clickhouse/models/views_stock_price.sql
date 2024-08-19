{{
    config(
        materialized="view"
    )
}}


-- Indicating recent stock price trend 
select 
    s as symbol 
    , fromUnixTimestamp(cast(t/1000 as int)) as date
    , avg(p) as price 
from 
    stock 
where 
    --date(fromUnixTimestamp(cast(t/1000 as int))) = 
    --(select 
    --    date(fromUnixTimestamp(cast(max(t)/1000 as int)))
    --from 
    --    stock
    --)
    --select correct real-time stock window
    fromUnixTimestamp(cast(t/1000 as int)) >= tumbleStart(now(), toIntervalDay('6'))
group by 1,2
order by 1,2