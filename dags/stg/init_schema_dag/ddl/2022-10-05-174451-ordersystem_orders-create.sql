CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);
CREATE INDEX IF NOT EXISTS IDX_ordersystem_orders__update_ts ON stg.ordersystem_orders (update_ts);