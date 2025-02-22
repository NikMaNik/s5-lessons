create table if not exists cdm.dm_settlement_report 
    (
        id serial NOT NULL ,
        restaurant_id varchar not null,
        restaurant_name varchar not null,
        settlement_date date not null,
        orders_count integer not null,
        orders_total_sum numeric(14, 2) not null,
        orders_bonus_payment_sum numeric(14, 2) not null,
        orders_bonus_granted_sum numeric(14, 2) not null,
        order_processing_fee numeric(14, 2) not null,
        restaurant_reward_sum numeric(14, 2) not null
    );

SELECT 
    sr.restaurant_id,
    sr.restaurant_name,
    sr.settlement_date,
    sr.orders_count,
    sr.orders_total_sum,
    sr.orders_bonus_payment_sum,
    sr.orders_bonus_granted_sum,
    sr.order_processing_fee,
    sr.restaurant_reward_sum
FROM cdm.dm_settlement_report sr
ORDER BY sr.settlement_date DESC;