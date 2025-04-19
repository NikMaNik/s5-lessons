import logging

import pendulum
from airflow.decorators import dag, task
from dags.examples.dds.dm_timestamps.dm_timestamps_loader import TimestamptLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds',  'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def sprint5_example_dds_timestamps_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # # Создаем подключение к базе подсистемы бонусов.
    # origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="timestamp_load")
    def load_restaurants():
        events_loader = TimestamptLoader(dwh_pg_connect, dwh_pg_connect, log)
        events_loader.load_timestmap()

    # Подключаем задачу загрузки событий в цепочку зависимостей
    dm_timestamp_task = load_restaurants()

    # Теперь порядок выполнения операций следующий:
    dm_timestamp_task
    

stg_bonus_system_ranks_dag = sprint5_example_dds_timestamps_dag()
