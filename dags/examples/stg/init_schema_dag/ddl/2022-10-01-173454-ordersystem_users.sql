create table if not exists stg.ordersystem_users
(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar not null UNIQUE,
    object_values text not null,
    update_ts timestamp not null
)