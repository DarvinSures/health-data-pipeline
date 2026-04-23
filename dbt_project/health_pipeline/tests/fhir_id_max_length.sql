-- Fails if any id exceeds 64 characters
select id
from {{ ref('fhir_patient') }}
where length(id) > 64