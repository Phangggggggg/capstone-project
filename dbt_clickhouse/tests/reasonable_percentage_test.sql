select 
*
from 
{{ ref('views_job_distribution') }}
where 
cases_pcent <0 or cases_pcent >1