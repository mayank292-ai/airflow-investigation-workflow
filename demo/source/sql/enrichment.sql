INSERT INTO output(business_key,value,page)
SELECT records.event_id, reference.label, 0
FROM records LEFT JOIN reference ON records.key = reference.key;
