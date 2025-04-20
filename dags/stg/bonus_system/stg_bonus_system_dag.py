import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder
from stg.bonus_system.event_loader import EventLoader
from stg.bonus_system.ranks_loader import RankLoader
from stg.bonus_system.users_loader import UserLoader
from stg.stg_settings_repository import StgEtlSettingsRepository

log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_case_stg_bonus_system',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
) as dag:

    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
    origin_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_ORIGIN_BONUS_SYSTEM_CONNECTION)

    settings_repository = StgEtlSettingsRepository(dwh_pg_connect)

    @task(task_id="ranks_dict_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect)
        rest_loader.load_ranks()

    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect)
        user_loader.load_users()

    ranks_dict = load_ranks()
    events = load_events()
    users = load_users()

    ranks_dict  # type: ignore
    events  # type: ignore
    users  # type: ignore
