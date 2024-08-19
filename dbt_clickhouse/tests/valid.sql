select 
*
from 
{{ ref('views_employer_distribution') }}
where 
length(company_name) <=0