SELECT *
FROM public_test.dm_settlement_report_actual a
FULL OUTER JOIN public_test.dm_settlement_report_expected e ON
    a.restaurant_id = e.restaurant_id AND
    a.settlement_year = e.settlement_year AND
    a.settlement_month = e.settlement_month
WHERE
    a.id IS NULL OR e.id IS NULL;