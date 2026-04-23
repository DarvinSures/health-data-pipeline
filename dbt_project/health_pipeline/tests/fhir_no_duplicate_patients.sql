-- Fails if duplicate patient IDs exist in fhir_patient
select id, count(*) as cnt
from {{ ref('fhir_patient') }}
group by id
having count(*) > 1