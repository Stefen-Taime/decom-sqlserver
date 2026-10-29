-- Execute through Warehouse — PostgreSQL (read only) in CloudBeaver.
-- This example pins the reference version validated for the first migration pilot.
SELECT *
FROM app.get_order_updates(
    '2016-05-30T00:00:00',
    '2016-06-01T00:00:00',
    'wwi-package-types/v1/70b20421d9e865772d2462dc897db708c6620e6ab6321e27581fe6f1b934f294'
)
ORDER BY "WWI Order ID"
LIMIT 20;
