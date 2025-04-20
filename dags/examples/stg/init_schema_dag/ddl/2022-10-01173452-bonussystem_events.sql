CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
    id SERIAL PRIMARY KEY,
    event_ts timestamp NOT NULL,
    event_type varchar Not NULL,
    event_value text NOT NULL
);