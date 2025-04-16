from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class OutboxOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_records(self, threshold: int, limit: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, record_ts as event_ts, type as event_type, payload as event_value
                    FROM outbox
                    WHERE id > %s -- берем только новые записи
                    ORDER BY id ASC -- упорядочиваем по возрастанию id
                    LIMIT %s; -- устанавливаем ограничение на число возвращаемых записей
                """,
                (threshold, limit),
            )
            records = cur.fetchall()
        return records
    


class EventDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )

class EventsLoader:
    WF_KEY = "example_outbox_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Число записей, которое будет обрабатывать за один запуск

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OutboxOriginRepository(pg_origin)
        self.stg = EventDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_records(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} records to process.")
            if not load_queue:
                self.log.info("No new data available.")
                return
            
            # Сохраняем преобразованные данные в целевую таблицу
            for event in load_queue:
                self.stg.insert_event(conn, event)
        
            # Сохраняем прогресс в настройках
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([r.id for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load completed up to ID {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}.")