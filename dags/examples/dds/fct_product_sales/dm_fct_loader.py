from logging import Logger
from typing import List
from datetime import datetime
from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class FctOriginRepository:
    def __init__(self, pg: PgConnect, log) -> None:
        self._db = pg
        self.log = log

    def list_product_sales(self, rank_threshold: int, limit: int):
        with self._db.client().cursor() as cur:
            self.log.info(f"{rank_threshold}")
            self.log.info(f"{limit}")
            cur.execute(
                """
                with product as 
                    (
                    select 
                        dr.id,
                        do2.order_key,
                        dp.product_id,
                        dp.product_price,
                        count(dp.product_id) as count
                    from 
                        dds.dm_products dp
                    join 
                        dds.dm_restaurants dr 
                        on
                            dp.restaurant_id  = dr.id
                    join 
                        dds.dm_orders do2 
                        on
                            do2.restaurant_id = dr.id
                    group by 
                        dr.id,
                        do2.order_key,
                        product_id,
                        product_price
                    limit
                        100
                    ),
                    bonus as 
                    (
                    select 
                        CAST(event_value::json->>'order_id' AS varchar) AS id,
                        cast(prod->>'bonus_payment'as float) as bonus_payment,
                        cast(prod->>'bonus_grant' as float) as bonus_grant
                    from 
                        stg.bonussystem_events,
                        LATERAL jsonb_array_elements(event_value::jsonb->'product_payments') as prod
                    )
                select 
                    dp.id,
                    do2.id,
                    p.count,
                    p.product_price,
                    p.product_price * p.count as total_sum,
                    b.bonus_payment,
                    b.bonus_grant
                from 
                    product p
                join dds.dm_products dp 
                    on
                        dp.product_id  = p.product_id
                join dds.dm_orders do2 
                    on
                        do2.order_key  = p.order_key
                join
                    bonus as b
                    on
                        p.order_key = b.id;
                """
            )

            objs = cur.fetchall()
        self.log.info(f"obj = {objs[0]}")
        return objs
    
class FctDestRepository:
    def insert_order(self, conn: Connection, fact, log) -> None:
        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO dds.fct_product_sales(
                            order_id,
                            product_id,
                            count,
                            price,
                            total_sum,
                            bonus_payment,
                            bonus_grant
                        )
                        VALUES (
                            %(order_id)s,
                            %(product_id)s,
                            %(count)s,
                            %(price)s,
                            %(total_sum)s,
                            %(bonus_payment)s,
                            %(bonus_grant)s
                        )
                        ON CONFLICT (order_id, product_id) DO UPDATE
                        SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant
                        ;
                    """,
                    {
                        "product_id": fact[0],
                        "order_id": fact[1],
                        "count": fact[2],
                        "price": fact[3],
                        "total_sum": fact[4],
                        "bonus_payment": fact[5],
                        "bonus_grant": fact[6]
                    },
                )

class FctLoader:
    WF_KEY = "example_fct_products_sales_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctOriginRepository(pg_origin, log)
        self.dds = FctDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_product(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_product_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.log.info(f"{row}")
                self.dds.insert_order(conn, row, self.log)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = 0
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.log)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
