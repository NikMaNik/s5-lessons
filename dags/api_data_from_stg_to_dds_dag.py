import logging
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import requests
import json
import psycopg2
from datetime import datetime

log = logging.getLogger(__name__)

conn = psycopg2.connect(
    dbname='de',
    user='jovyan',
    password='jovyan',
    host='localhost',
    port='5432'
)

def get_couriers_from_stg():
    with conn.cursor() as cur:
        cur.execute('''SELECT object_id AS courier_id, 
	                                          object_value->>'name' AS courier_name
                                        FROM stg.api_couriers; ''')
        
        couriers_list = cur.fetchall()
        return couriers_list


def insert_couriers_into_dds():
    couriers_list = get_couriers_from_stg()
    for courier in couriers_list:
        courier_key = courier[0]
        courier_name = courier[1]
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO dds.dm_couriers (courier_key, courier_name)
                    VALUES(%(courier_key)s, %(courier_name)s)
                    ON CONFLICT (courier_key) DO UPDATE
                    SET
                        courier_key = EXCLUDED.courier_key;
    """,{
                    "courier_key": courier_key,
                    "courier_name": courier_name
                }
                )        
    conn.commit()

def get_deliveries_from_stg():
    with conn.cursor() as cur:
        cur.execute('''SELECT ad.object_id AS delivery_key,
							  ad.object_value->>'order_id' AS order_key,
                              ad.object_value->>'sum' AS delivery_sum,
                              ad.object_value->>'order_ts' AS order_ts,
                              ad.object_value->>'delivery_ts' AS delivery_ts,
                              ad.object_value->>'address' AS address,
	                          dmc.id  AS courier_id,
	                          ad.object_value->>'rate' AS rate,
	                          ad.object_value->>'tip_sum' AS tip_sum
	                          FROM stg.api_deliveries AS ad
                        INNER JOIN dds.dm_orders AS dmo ON ad.object_value->>'order_id' = dmo.order_key
                        INNER JOIN dds.dm_couriers AS dmc ON ad.object_value->>'courier_id' = dmc.courier_key;''')
        deliveries_list = cur.fetchall()
        return deliveries_list


def insert_deliveries_into_dds():
    deliveries_list = get_deliveries_from_stg()
    for delivery in deliveries_list:
        delivery_key = delivery[0]
        order_key = delivery[1]
        delivery_sum = delivery[2]
        order_ts = delivery[3]
        delivery_ts = delivery[4]
        courier_id = delivery[5]
        address = delivery[6]
        rate = delivery[7]
        tip_sum = delivery[8]
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO dds.dm_deliveries (delivery_key, order_key, delivery_sum, order_ts,	
                                                          delivery_ts, address, courier_id, rate, tip_sum)
                    VALUES( %(delivery_key)s, %(order_key)s, %(delivery_sum)s, %(order_ts)s,
                          %(delivery_ts)s, %(courier_id)s,  %(address)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (delivery_key) DO UPDATE
                    SET
                        delivery_key = EXCLUDED.delivery_key,
                        order_key = EXCLUDED.order_key,
                        delivery_sum = EXCLUDED.delivery_sum,
                        order_ts = EXCLUDED.order_ts,
                        delivery_ts = EXCLUDED.delivery_ts,
                        address = EXCLUDED.address,
                        courier_id = EXCLUDED.courier_id,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum
    """,{
                    "delivery_key": delivery_key,
                    "order_key": order_key,
                    "delivery_sum": delivery_sum,
                    "order_ts": order_ts,
                    "delivery_ts": delivery_ts,
                    "address": address,
                    "courier_id": courier_id,
                    "rate": rate,
                    "tip_sum": tip_sum
                }
                )           
    conn.commit()

def get_data_for_fct_couriers_deliveries():
    with conn.cursor() as cur:
        cur.execute('''SELECT dmo.id AS order_id,
                              dmd.id AS delivery_id,
                              dmd.order_ts::timestamp AS order_ts,
	                          delivery_ts::timestamp AS delivery_ts,
	                          delivery_sum AS delivery_price,
	                          courier_id AS courier_id,
	                          dmc.courier_name AS courier_name,
	                          rate AS rate,
	                          tip_sum AS tip_sum
                        FROM dds.dm_deliveries AS dmd
                        INNER JOIN dds.dm_couriers AS dmc ON dmc.id = dmd.courier_id
                        INNER JOIN dds.dm_orders AS dmo ON dmd.order_key = dmo.order_key;''')
        data_for_fact_table = cur.fetchall()
        return data_for_fact_table

def insert_data_in_fct_couriers_deliveries(data_for_fct_couriers_deliveries): 
    data_for_fact_table = get_data_for_fct_couriers_deliveries()
    for courier_data in data_for_fct_couriers_deliveries:
        order_id = courier_data[0]
        delivery_id = courier_data[1]
        order_ts = courier_data[2]
        delivery_ts = courier_data[3]
        delivery_price = courier_data[4]
        courier_id = courier_data[5]
        courier_name = courier_data[6]
        rate = courier_data[7]
        tip_sum = courier_data[8]
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO dds.fct_couriers_deliveries(order_id, delivery_id, order_ts, delivery_ts, delivery_price,
                                                                courier_id, courier_name, rate, tip_sum)
                         VALUES(%(order_id)s, %(delivery_id)s, %(order_ts)s, %(delivery_ts)s, %(delivery_price)s, %(courier_id)s,
                          %(courier_name)s, %(rate)s,  %(tip_sum)s)
                         ON CONFLICT (delivery_id, courier_id, order_id) DO UPDATE
                         SET
                            order_id = EXCLUDED.order_id,
                            delivery_id = EXCLUDED.delivery_id,
                            order_ts = EXCLUDED.order_ts::timestamp,
                            delivery_ts = EXCLUDED.delivery_ts::timestamp,
                            delivery_price = EXCLUDED.delivery_price,
                            courier_id = EXCLUDED.courier_id,
                            courier_name = EXCLUDED.courier_name,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum
                        ''',{
                                "order_id":order_id,
                                "delivery_id":delivery_id,
                                "order_ts":order_ts,
                                "delivery_ts":delivery_ts,
                                "delivery_price":delivery_price,
                                "courier_id":courier_id,
                                "courier_name":courier_name,
                                "rate":rate,
                                "tip_sum":tip_sum
                            }
                            )
    conn.commit()        

dag = DAG(dag_id='api_data_from_stg_to_dds_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['this dag for load data from stg to dds dag'],
    is_paused_upon_creation=False)

task_1 = PythonOperator(task_id='get_couriers_from_stg',
                      python_callable=get_couriers_from_stg,
                      dag=dag)   

task_2 = PythonOperator(task_id='insert_couriers_into_dds',
                      python_callable=insert_couriers_into_dds,
                      dag=dag)   

task_3 = PythonOperator(task_id='get_deliveries_from_stg',
                      python_callable=get_deliveries_from_stg,
                      dag=dag)   

task_4 = PythonOperator(task_id='insert_deliveries_into_dds',
                      python_callable=insert_deliveries_into_dds,
                      dag=dag)   

task_5 = PythonOperator(task_id='get_data_for_fct_couriers_deliveries',
                      python_callable=get_data_for_fct_couriers_deliveries,
                      dag=dag)   

task_6 = PythonOperator(task_id='insert_data_in_fct_couriers_deliveries',
                      python_callable=insert_data_in_fct_couriers_deliveries,
                      dag=dag)   

task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6