select id
from {{ source('raw', 'raw_patient') }}
where birth_date > current_date