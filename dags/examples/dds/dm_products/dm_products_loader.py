from logging import Logger
from typing import List
from datetime import datetime
from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductObj(BaseModel):
    id: int
    _id: str
    menu: str
    name: str
    update_ts: datetime


class ProductOriginRepository:
    def __init__(self, pg: PgConnect, log) -> None:
        self._db = pg
        self.log = log

    def list_products(self, rank_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor() as cur:
            self.log.info(f"{rank_threshold}")
            self.log.info(f"{limit}")
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, 
                {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )

            objs = cur.fetchall()
            
            self.log.info(f"obj = {objs}")
            result = []
            max_id = 0
            for row in objs:
                obj = str2json(row[1])
                max_id = row[0]
                result.append(obj)
            self.log.info(f"obj = {result}")
            self.log.info(f"max_id = {max_id}")
        return result, max_id
    


class ProductDestRepository:

    def insert_product(self, conn: Connection, rank, log) -> None:
        for order_item in rank['order_items']:

            log.info(f'''
                        "product_id": {order_item['id']},
                        "product_name": {order_item['name']},
                        "product_price": {order_item['price']},
                        "restaurant_id": {rank['restaurant_id']}''')
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                        VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s);
    
                    """,
                    {
                        "restaurant_id": rank['restaurant_id'],
                        "product_id": rank['id'],
                        "product_name": rank['name'],
                        "product_price": rank['price'],
                        "active_from": rank['update_ts'],
                        "active_to" :datetime(2099, 12, 31, 0, 0, 0)
                    },
                )

class ProductLoader:
    WF_KEY = "example_dm_product_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductOriginRepository(pg_origin, log)
        self.dds = ProductDestRepository()
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
            load_queue, max_id = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.log.info(f"{user}")
                self.dds.insert_product(conn, user, self.log)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max_id
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, self.log)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
