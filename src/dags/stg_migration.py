import pendulum
import datetime as dt
import logging
import vertica_python

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator


CONFIG = {
    'users': {
        'filename': '/data/users.csv',
        'table': 'VVJAKELYANDEXRU__STAGING.users',
        'insert_query2': """
            COPY VVJAKELYANDEXRU__STAGING.users (id,chat_name ENFORCELENGTH,registration_dt,country ENFORCELENGTH,age)
            FROM LOCAL '/data/users.csv'
            DELIMITER ';'
            REJECTED DATA AS TABLE VVJAKELYANDEXRU__STAGING.users_rej
            ;
            """,
        'insert_query': """
            COPY VVJAKELYANDEXRU__STAGING.users (id,chat_name ENFORCELENGTH,registration_dt,country ENFORCELENGTH,age)
            FROM LOCAL '/data/users.csv'
            PARSER fcsvparser(delimiter=',', enclosed_by='"')
            REJECTED DATA AS TABLE VVJAKELYANDEXRU__STAGING.users_rej
            ;
            """
    },
    'groups': {
        'filename': '/data/groups.csv',
        'table': 'VVJAKELYANDEXRU__STAGING.groups',
        'insert_query2': """
            COPY VVJAKELYANDEXRU__STAGING.groups (id,admin_id,group_name ENFORCELENGTH,registration_dt,is_private)
            FROM LOCAL '/data/groups.csv'
            DELIMITER ';'
            REJECTED DATA AS TABLE VVJAKELYANDEXRU__STAGING.groups_rej
            ;
            """,
        'insert_query': """
            COPY VVJAKELYANDEXRU__STAGING.groups (id,admin_id,group_name ENFORCELENGTH,registration_dt,is_private)
            FROM LOCAL '/data/groups.csv'
            PARSER fcsvparser(delimiter=',', enclosed_by='"')
            REJECTED DATA AS TABLE VVJAKELYANDEXRU__STAGING.groups_rej
            ;
            """
    },
    'dialogs': {
        'filename': '/data/dialogs.csv',
        'table': 'VVJAKELYANDEXRU__STAGING.dialogs',
        'insert_query2': """
            COPY VVJAKELYANDEXRU__STAGING.dialogs (message_id,message_ts,message_from,message_to,message ENFORCELENGTH,message_group)
            FROM LOCAL '/data/dialogs.csv'
            DELIMITER ';'
            REJECTED DATA AS TABLE VVJAKELYANDEXRU__STAGING.dialogs_rej
            ;
            """,
        'insert_query': """
            COPY VVJAKELYANDEXRU__STAGING.dialogs (message_id,message_ts,message_from,message_to,message ENFORCELENGTH,message_group)
            FROM LOCAL '/data/dialogs.csv'
            PARSER fcsvparser(delimiter=',', enclosed_by='"')
            REJECTED DATA AS TABLE VVJAKELYANDEXRU__STAGING.dialogs_rej
            ;
            """
    },
    'group_log': {
        'filename': '/data/group_log.csv',
        'table': 'VVJAKELYANDEXRU__STAGING.group_log',
        'insert_query': """
            COPY VVJAKELYANDEXRU__STAGING.group_log (group_id, user_id, user_id_from, event, "datetime")
            FROM LOCAL '/data/group_log.csv'
            PARSER fcsvparser(delimiter=',', enclosed_by='"')
            REJECTED DATA AS TABLE VVJAKELYANDEXRU__STAGING.group_log_rej
            ;
            """
    },
}

CONN_INFO = {'host': '51.250.75.20',
             'port': 5433,
             'user': 'VVJAKELYANDEXRU',
             'password': 'mRgWsjRg8XdppiH',
             'database': 'dwh'}


def create_tables(conn_info: dict) -> None:
    query = """
    drop table if exists VVJAKELYANDEXRU__STAGING.dialogs;
    drop table if exists VVJAKELYANDEXRU__STAGING.groups;
    drop table if exists VVJAKELYANDEXRU__STAGING.users;
    drop table if exists VVJAKELYANDEXRU__STAGING.group_log;
    
    create table if not exists VVJAKELYANDEXRU__STAGING.users (
        id integer primary key,
        chat_name varchar(200),
        registration_dt timestamp(0),
        country varchar(200),
        age integer
    )
    order by id
    segmented by hash(id) all nodes
    -- partition by age
    ;

    create table if not exists VVJAKELYANDEXRU__STAGING.groups (
        id integer primary key,
        admin_id integer,
        group_name varchar(100),
        registration_dt timestamp(6),
        is_private integer,
        foreign key (admin_id) references VVJAKELYANDEXRU__STAGING.users,
        constraint groups_check_is_private check(is_private in (0, 1))
    )
    order by id, admin_id
    segmented by hash(id) all nodes
    PARTITION BY registration_dt::date
    GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2)
    ;

    create table if not exists VVJAKELYANDEXRU__STAGING.dialogs (
        message_id integer primary key,
        message_ts timestamp(9),
        message_from integer,
        message_to integer,
        message varchar(1000),
        message_group integer
    )
    order by message_id
    segmented by hash(message_id) all nodes
    partition by message_ts::date
    GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)
    ;

    create table VVJAKELYANDEXRU__STAGING.group_log (
    group_id integer PRIMARY KEY not null,
    user_id integer REFERENCES  users(id),
    user_id_from integer default null,
    event varchar(6),
    "datetime" timestamp(6)
    )
    order by group_id
    SEGMENTED BY hash(group_id) all nodes
    PARTITION BY "datetime"::date
    GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);
    """
    
    conn = vertica_python.connect(**conn_info)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    finally:
        conn.close()
    

def migr_to_stg(conn_info: dict, query: str) -> None:
    conn = vertica_python.connect(**conn_info)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    finally:
        conn.close()


args = {
    "owner": "dosperados",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


with DAG(
        'stg_migration',
        default_args=args,
        description='DWH migration data from csv files to vertica',
        catchup=True,
        schedule_interval=None,
        start_date=pendulum.datetime(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day, dt.datetime.now().hour, tz="UTC"),
        tags=['Sprint6', 'de-project-5', 'stg'],
        is_paused_upon_creation=True,
) as dag:
    # start_task = DummyOperator(task_id='start')
    start_task = PythonOperator(
        task_id='start',
        python_callable=create_tables,
        op_kwargs={'conn_info': CONN_INFO}
    )
    
    loading_users_task = PythonOperator(
        task_id='loading_users',
        python_callable=migr_to_stg,
        op_kwargs={'conn_info': CONN_INFO, 'query': CONFIG['users']['insert_query']}
    )

    loading_groups_task = PythonOperator(
        task_id='loading_groups',
        python_callable=migr_to_stg,
        op_kwargs={'conn_info': CONN_INFO, 'query': CONFIG['groups']['insert_query']}
    )

    loading_group_log_task = PythonOperator(
        task_id='loading_group_log',
        python_callable=migr_to_stg,
        op_kwargs={'conn_info': CONN_INFO, 'query': CONFIG['group_log']['insert_query']}
    )

    loading_dialogs_task = PythonOperator(
        task_id='loading_dialogs',
        python_callable=migr_to_stg,
        op_kwargs={'conn_info': CONN_INFO, 'query': CONFIG['dialogs']['insert_query']}
    )
   
    finish_task = DummyOperator(task_id='end')
    
    (
    start_task
    >> loading_users_task
    >> loading_groups_task
    >> loading_group_log_task
    >> loading_dialogs_task
    >> finish_task
    )
