create table if Not exists stg.bonussystem_users 
(
    id INTEGER primary key,
    order_user_id text not null
);