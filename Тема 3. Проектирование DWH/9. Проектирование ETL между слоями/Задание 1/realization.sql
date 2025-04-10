INSERT INTO cdm.dm_settlement_report (
    id,
    settlement_date,
    restaurant_id,
    total_order_amount,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT
    o.order_id AS settlement_id,
    o.order_closed_at::date AS settlement_date,
    r.restaurant_id,
    SUM(o.total_amount) AS total_order_amount,
    SUM(o.total_amount * 0.25) AS order_processing_fee,
    SUM(o.total_amount - o.total_amount * 0.25 - COALESCE(bonus_payment_amount, 0)) AS restaurant_reward_sum
FROM orders o
JOIN restaurants r ON o.restaurant_id = r.restaurant_id
LEFT JOIN bonus_payments bp ON o.order_id = bp.order_id
WHERE o.status = 'closed'
GROUP BY o.order_id, o.order_closed_at::date, r.restaurant_id
ON CONFLICT (settlement_id, settlement_date, restaurant_id) DO UPDATE SET
    total_order_amount = EXCLUDED.total_order_amount,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;