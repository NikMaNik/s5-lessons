from examples.repositories.pg_connect import PgConnect


class SettlementRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_settlement_by_days(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
WITH aggregated_data AS (
    SELECT
        "dr".restaurant_id,
        "dr".restaurant_name,
        "dt".date::date as settlement_date,
        COUNT(fps.count) as orders_count,
        SUM(fps.total_sum) as orders_total_sum,
        SUM(fps.bonus_payment) as orders_bonus_payment_sum,
        SUM(fps.bonus_grant) as orders_bonus_granted_sum,
        SUM(fps.total_sum * 0.25) as order_processing_fee,
        SUM(fps.total_sum - fps.total_sum * 0.25 - fps.bonus_payment) as restaurant_reward_sum
    FROM dds.fct_product_sales as fps
    JOIN dds.dm_orders as "do" on fps.order_id = "do".id 
    JOIN dds.dm_products as "dp" on fps.product_id = "dp".id
    JOIN dds.dm_restaurants as "dr" on "dp".restaurant_id = "dr".id
    JOIN dds.dm_timestamps as "dt" on "do".timestamp_id = "dt".id
    WHERE 
        "do".order_status = 'CLOSED'
    GROUP BY
        "dr".restaurant_id,
        "dr".restaurant_name,
        "dt".date::date
    ORDER BY 
        settlement_date
)
INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
FROM aggregated_data;
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
                    """
                )
                conn.commit()


class SettlementReportLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = SettlementRepository(pg)

    def load_report_by_days(self):
        self.repository.load_settlement_by_days()
