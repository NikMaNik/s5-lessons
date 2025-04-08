insert into dm_settlement_report
SELECT 
      dr.restaurant_id
    , dr.restaurant_name
    , dt.date as settlement_report
    , fps.count as orders_count
    , fps.total_sum as orders_total_sum
    , fps.bonus_payment as orders_bonus_payment_sum
    , fps.bonus_grant as orders_bonus_granted_sum
    , fps.orders_total_sum * 0.25 as order_processing_fee
    , fps.count - fps.orders_total_sum * 0.25 - fps.bonus_payment as restaurant_reward_sum
FROM dds.fct_product_sales as fps
JOIN dds.dm_orders as 'do' on fps.order_id = do.id 
JOIN dds.dm_products as 'dp' on fps.product_id = dp.id
JOIN dds.dm_restaurants as 'dr' on dp.restaurant_id = dr.id
JOIN dds.dm_timestamps as 'dt' on dm.timestamp_id = dt.id;
