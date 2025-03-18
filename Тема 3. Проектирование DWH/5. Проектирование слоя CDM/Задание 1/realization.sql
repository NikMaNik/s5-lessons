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
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    column_default,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;