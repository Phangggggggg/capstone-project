select 
*
from 
{{ ref('views_state_distribution') }}
where 
avg_wage <=0