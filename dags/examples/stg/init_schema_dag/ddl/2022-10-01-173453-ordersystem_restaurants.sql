create table if not exists stg.ordersystem_restaurants
(
    id serial primary key ,
    object_id varchar not null,
    object_values text not null,
    update_ts timestamp not null
)