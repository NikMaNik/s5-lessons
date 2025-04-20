import logging

import pendulum
from airflow.decorators import dag, task
log = logging.getLogger(__name__)




@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'settlement'],
    is_paused_upon_creation=False
)


def some_f():
    date = '{{ ds }}'
    @task
    def same_task():
        log.info(f"SOME DATE = {date}");

    same_task()  # type: ignore


my_dag = some_f()