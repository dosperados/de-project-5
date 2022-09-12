import boto3
import pendulum
import datetime as dt
import logging
import pandas as pd
import os #для создания папки

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

# Функция создает папку для сохранения файлов в контенере если ее нет
def create_folder():
    path = os.getcwd()
    print ("The current working directory is %s" % path)
    path = '/data'
    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
    else:
        print ("Successfully created the directory %s " % path)

logger = logging.getLogger(__name__)

def get_file_from_s3(file_name: str):
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
    FILENAME = '/data/'+file_name

    print(f'FILENAME: {FILENAME}')
    print(f'file_name: {file_name}')

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
    s3_client.download_file(
        Bucket='sprint6',
        Key=file_name,
        Filename=FILENAME
        )
    logging.info(s3_client. head_object (Bucket='sprint6', Key=file_name),)
    logging.info(f'\n Header in {FILENAME}: \n{pd.read_csv(FILENAME, nrows=10)}')


args = {
    "owner": "dosperados",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

bucket_files = ['group.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']

with DAG(
        'de-project-5_stg_fetch_files_from_s3',
        default_args=args,
        description='DWH fetch files from s3 to local path',
        catchup=True,
        schedule_interval=None,
        #schedule_interval='20,40 * * * *',
        start_date=pendulum.datetime(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day, dt.datetime.now().hour, tz="UTC"),
        tags=['Sprint6', 'de-project-5', 'stg', 'cdm'],
        is_paused_upon_creation=True,
) as dag:
    start_task = PythonOperator(
    task_id='stg_migr_start',
    python_callable=create_folder,
    op_kwargs={}
    )

    fetch_groups = PythonOperator(
    task_id='fetch_groups.csv',
    python_callable=get_file_from_s3,
    op_kwargs={'file_name': 'groups.csv'}
    )

    fetch_users= PythonOperator(
    task_id='fetch_users.csv',
    python_callable=get_file_from_s3,
    op_kwargs={'file_name': 'users.csv'}
    )

    fetch_dialogs= PythonOperator(
    task_id='fetch_dialogs.csv',
    python_callable=get_file_from_s3,
    op_kwargs={'file_name': 'dialogs.csv'}
    )

    fetch_group_log= PythonOperator(
    task_id='fetch_group_log.csv',
    python_callable=get_file_from_s3,
    op_kwargs={'file_name': 'group_log.csv'}
    )


start_task >> [fetch_groups, fetch_users, fetch_dialogs, fetch_group_log] 
