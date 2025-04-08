INSERT INTO cdm.dm_settlement_report (
    id,
    restaurant_id,
    restaurant_name,
    settlement_report,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT 
    fps.id,
    "do".restaurant_id,
    "dr".restaurant_name,
    "dt".date as settlement_report,
    fps.count as orders_count,
    fps.total_sum as orders_total_sum,
    fps.bonus_payment as orders_bonus_payment_sum,
    fps.bonus_grant as orders_bonus_granted_sum,
    fps.total_sum * 0.25 as order_processing_fee,
    fps.total_sum - fps.total_sum * 0.25 - fps.bonus_payment as restaurant_reward_sum
FROM dds.fct_product_sales as fps
JOIN dds.dm_orders as "do" on fps.order_id = "do".id 
JOIN dds.dm_products as "dp" on fps.product_id = "dp".id
JOIN dds.dm_restaurants as "dr" on "dp".restaurant_id = "dr".id
JOIN dds.dm_timestamps as "dt" on "do".timestamp_id = "dt".id
ON CONFLICT (restaurant_id, settlement_report) DO UPDATE SET
    id = EXCLUDED.id
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;