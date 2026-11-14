select * from (values
    ('yellow', 'Yellow taxi', 'Medallion taxis; street hails anywhere in NYC, trip records via TPEP'),
    ('green',  'Green taxi',  'Boro taxis; street hails outside central Manhattan and the airports, trip records via LPEP')
) as s (service_type, service_name, description)
