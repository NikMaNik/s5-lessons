insert into cdm.dm_settlement_report
SELECT 
    fps.id
    , "do".restaurant_id
    , "dr".restaurant_name
    , "dt".date as settlement_report
    , COUNT(fps.count) as orders_count
    , SUM(fps.total_sum) as orders_total_sum
    , SUM(fps.bonus_payment) as orders_bonus_payment_sum
    , SUM(fps.bonus_grant) as orders_bonus_granted_sum
    , SUM(fps.total_sum) * 0.25 as order_processing_fee
    , SUM(fps.total_sum) - SUM(fps.total_sum) * 0.25 - SUM(fps.bonus_payment) as restaurant_reward_sum
FROM dds.fct_product_sales as fps
JOIN dds.dm_orders as "do" on fps.order_id = "do".id 
JOIN dds.dm_products as "dp" on fps.product_id = "dp".id
JOIN dds.dm_restaurants as "dr" on "dp".restaurant_id = "dr".id
JOIN dds.dm_timestamps as "dt" on "do".timestamp_id = "dt".id
GROUP BY 1,2,3,4;
