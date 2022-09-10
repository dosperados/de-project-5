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
    'l_user_group_activity': {
        'table': 'VVJAKELYANDEXRU__DWH.l_user_group_activity',
        'insert_query': """
            INSERT INTO VVJAKELYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
            select distinct
            hash(hu.user_id, hg.group_id)  as hk_l_user_group_activity,
            hu.hk_user_id,
            hg.hk_group_id,
            now() as load_dt,
	        's3' as load_src
        from VVJAKELYANDEXRU__STAGING.group_log as gl
        left join VVJAKELYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id 
        left join VVJAKELYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
        where hash(hu.user_id, hg.group_id) not in (select hk_l_user_group_activity from VVJAKELYANDEXRU__DWH.l_user_group_activity)
        ;
        """
    },
    's_auth_history': {
        'table': 'VVJAKELYANDEXRU__DWH.s_auth_history',
        'insert_query': """
        INSERT INTO VVJAKELYANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
        select 
        luga.hk_l_user_group_activity,
        gl.user_id_from,
        gl.event,
        gl."datetime" as event_dt,
        now() as load_dt,
        's3' as load_src
        from VVJAKELYANDEXRU__STAGING.group_log as gl
        left join VVJAKELYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
        left join VVJAKELYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
        left join VVJAKELYANDEXRU__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
        where luga.hk_l_user_group_activity not in (select distinct hk_l_user_group_activity from VVJAKELYANDEXRU__DWH.s_auth_history)
        ;
        """
    },
    'dm_user_conversion': {
        'table': 'VVJAKELYANDEXRU__DWH.dm_user_conversion',
        'insert_query': """
        truncate table VVJAKELYANDEXRU__DWH.dm_user_conversion;

        INSERT INTO VVJAKELYANDEXRU__DWH.dm_user_conversion (hk_group_id, cnt_added_users, cnt_users_in_group_with_messages, group_conversion, load_dt)
        with last_10_groups as (
            select hg.hk_group_id, hg.group_id 
                from VVJAKELYANDEXRU__DWH.h_groups hg 
                order by hg.registration_dt asc
                limit 10
        ), users_added_to_group as (
            select luga2.hk_group_id, count(distinct luga2.hk_user_id) as count from VVJAKELYANDEXRU__DWH.l_user_group_activity luga2 
            inner join VVJAKELYANDEXRU__DWH.s_auth_history sah2 on luga2.hk_l_user_group_activity = sah2.hk_l_user_group_activity 
            where luga2.hk_group_id in (select hk_group_id from last_10_groups)
                and sah2.event = 'add'	
            group by luga2.hk_group_id
        ), user_group_messages as (
            select hg.hk_group_id,
                    max(added_u.count) as cnt_added_users,
                    count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
            FROM last_10_groups hg 
            inner join users_added_to_group as added_u on hg.hk_group_id = added_u.hk_group_id
            inner join VVJAKELYANDEXRU__DWH.l_groups_dialogs lgd on hg.hk_group_id = lgd.hk_group_id
            inner join VVJAKELYANDEXRU__DWH.l_user_message lum on lum.hk_message_id = lgd.hk_message_id
            left join VVJAKELYANDEXRU__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id and lum.hk_user_id = luga.hk_user_id
            left join VVJAKELYANDEXRU__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
            group by hg.hk_group_id
        ), main as (
            select *, (cnt_users_in_group_with_messages / cnt_added_users) as group_conversion from user_group_messages
        )
        select hk_group_id, cnt_added_users, cnt_users_in_group_with_messages, group_conversion, now() as load_dt  from main order by group_conversion desc
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
    start_task = DummyOperator(task_id='start')

    create_tables_task = PythonOperator(
        task_id='create_tables',
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

    loading_l_user_group_activity_task = PythonOperator(
        task_id='loading_l_user_group_activity',
        python_callable=migr_to_stg,
        op_kwargs={'conn_info': CONN_INFO, 'query': CONFIG['l_user_group_activity']['insert_query']}
    )

    loading_s_auth_history_task = PythonOperator(
        task_id='loading_s_auth_history',
        python_callable=migr_to_stg,
        op_kwargs={'conn_info': CONN_INFO, 'query': CONFIG['s_auth_history']['insert_query']}
    )

    loading_dm_user_conversion_task = PythonOperator(
        task_id='loading_dm_user_conversion',
        python_callable=migr_to_stg,
        op_kwargs={'conn_info': CONN_INFO, 'query': CONFIG['dm_user_conversion']['insert_query']}
    )
   
    finish_task = DummyOperator(task_id='end')
    
    (
    start_task
    >> create_tables_task
    >> loading_users_task
    >> loading_groups_task
    >> loading_group_log_task
    >> loading_dialogs_task
    >> loading_l_user_group_activity_task
    >> loading_s_auth_history_task
    >> loading_dm_user_conversion_task
    >> finish_task
    )
