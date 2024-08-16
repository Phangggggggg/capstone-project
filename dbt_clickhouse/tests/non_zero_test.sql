select 
count(*) as cnt
from 
{{ ref('views_case_cnt_d') }}
having cnt <=0