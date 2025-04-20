DO $do$ BEGIN IF EXISTS (
    SELECT
    FROM pg_catalog.pg_roles
    WHERE rolname = 'sp5_de_tester'
) THEN
GRANT SELECT ON all tables IN SCHEMA stg TO sp5_de_tester;
END IF;
END $do$;