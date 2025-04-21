SELECT 
    current_timestamp::timestamp with time zone AS test_date_time,
    'test_01' AS test_name,
    CASE
        WHEN COUNT(*) > 0 THEN True  -- Если есть хотя бы одно расхождение, тест неуспешен
        ELSE False                   -- Иначе тест успешен
    END AS test_result
FROM 
    public_test.dm_settlement_report_actual a
FULL OUTER JOIN 
    public_test.dm_settlement_report_expected e 
ON
    a.restaurant_id = e.restaurant_id AND
    a.settlement_year = e.settlement_year AND
    a.settlement_month = e.settlement_month
WHERE
    a.id IS NULL OR e.id IS NULL;