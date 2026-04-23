-- Fails if any fhir_patient row is missing phone or email in telecom
select id
from consumption.fhir_patient
where telecom->>'phone' is null
   or telecom->>'email' is null