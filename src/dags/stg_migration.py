import json
import psycopg2 as pg
import requests
import pendulum
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime


API_ADDR = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
API_HEADERS={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": "ilya",
    "X-Cohort": "2"
}


def get_pg_conn():
    conn = pg.connect(
        host='localhost', 
        port='5432', 
        dbname='de',
        user='jovyan',
        password='jovyan'
    )
    return conn


CONFIG = {
    'restaurants': {
        'api_address': API_ADDR,
        'api_endpoint': 'restaurants',
        'api_headers': API_HEADERS,
        'db_conn': get_pg_conn,
        'record_id': '_id',
        'step': 5,
        'from_ts': False,
    },
    'couriers': {
        'api_address': API_ADDR,
        'api_endpoint': 'couriers',
        'api_headers': API_HEADERS,
        'db_conn': get_pg_conn,
        'record_id': '_id',
        'step': 30,
        'from_ts': False,
    },
    'deliveries': {
        'api_address': API_ADDR,
        'api_endpoint': 'deliveries',
        'api_headers': API_HEADERS,
        'db_conn': get_pg_conn,
        'record_id': 'order_id',
        'step': 50,
        'from_ts': True,
    }
}


def get_last_delivery_ts(db_conn: pg.connect):
    query = """
    select 
        date_trunc(
            'second', 
            coalesce(
                max(d.object_value::json ->> 'order_ts')::timestamp, 
                '1900-01-01'::timestamp
            )
        ) - interval '3 day'
    from stg.deliveries d
    """  # беру временной лаг в 3 дня для того, что бы забирать заказы, которые могут измениться в течение 3 дней
    
    with db_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        last_ts = cursor.fetchone()[0]
    
    return last_ts
        



def uni_migration(
    api_address: str, 
    api_endpoint: str, 
    api_headers: dict,
    db_conn: pg.connect,
    record_id: str,
    step: int,
    from_ts: bool = False
) -> None:
    start_i = 0
    step_i = step
    data_exists = True
    
    if from_ts:
        lst_delivery = get_last_delivery_ts(db_conn=db_conn)
        extra_api = f"&from={lst_delivery}"
    else:
        extra_api = ""
    
    
    while data_exists:
        response: list[dict] = requests.get(
            f"{api_address}/{api_endpoint}?offset={start_i}&limit={step_i}" + extra_api, 
            headers=api_headers
        ).json()
        
        if len(response) == 0:
            data_exists = False
        else:
            with db_conn() as conn:
                cursor = conn.cursor()
                
                for row in response:
                    values = []
                    
                    # set object_id
                    object_id = row.pop(record_id, None)
                    values.append(object_id)
                    
                    # set object_value
                    object_value = json.dumps(row)
                    values.append(object_value)
                    
                    insert_query = f"""
                    insert into stg.{api_endpoint} (object_id, object_value)
                    values (%s, %s)
                    on conflict (object_id) do update set 
                    object_value = excluded.object_value
                    """
                    cursor.execute(insert_query, values)
                
                conn.commit()
            
            start_i += step_i


args = {
    "owner": "ilya",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


with DAG(
        'stg_migration',
        default_args=args,
        description='DWH migration data to stg layer',
        catchup=True,
        schedule_interval='0/10 * * * *',
        start_date=pendulum.datetime(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day, dt.datetime.now().hour, tz="UTC"),
        tags=['de-project-4', 'dwh', 'stg'],
        is_paused_upon_creation=True,
) as dag:
    start_task = DummyOperator(task_id='stg_migr_start')
    
    migr_restaurants_task = PythonOperator(
        task_id='migr_restaurants',
        python_callable=uni_migration,
        op_kwargs={'api_address': CONFIG['restaurants']['api_address'],
                   'api_endpoint': CONFIG['restaurants']['api_endpoint'],
                   'api_headers': CONFIG['restaurants']['api_headers'],
                   'db_conn': CONFIG['restaurants']['db_conn'],
                   'record_id': CONFIG['restaurants']['record_id'],
                   'step': CONFIG['restaurants']['step'],
                   'from_ts': CONFIG['restaurants']['from_ts']}
    )
    
    migr_couriers_task = PythonOperator(
        task_id='migr_couriers',
        python_callable=uni_migration,
        op_kwargs={'api_address': CONFIG['couriers']['api_address'],
                   'api_endpoint': CONFIG['couriers']['api_endpoint'],
                   'api_headers': CONFIG['couriers']['api_headers'],
                   'db_conn': CONFIG['couriers']['db_conn'],
                   'record_id': CONFIG['couriers']['record_id'],
                   'step': CONFIG['couriers']['step'],
                   'from_ts': CONFIG['couriers']['from_ts']}
    )

    migr_deliveries_task = PythonOperator(
        task_id='migr_deliveries',
        python_callable=uni_migration,
        op_kwargs={'api_address': CONFIG['deliveries']['api_address'],
                   'api_endpoint': CONFIG['deliveries']['api_endpoint'],
                   'api_headers': CONFIG['deliveries']['api_headers'],
                   'db_conn': CONFIG['deliveries']['db_conn'],
                   'record_id': CONFIG['deliveries']['record_id'],
                   'step': CONFIG['deliveries']['step'],
                   'from_ts': CONFIG['deliveries']['from_ts']}
    )
    
    finish_task = DummyOperator(task_id='stg_migr_finish')
    
    (
    start_task
    >> [migr_restaurants_task, migr_couriers_task, migr_deliveries_task]
    >> finish_task
    )
