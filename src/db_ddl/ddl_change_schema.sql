/* DDL Создания необходимых таблиц и связей */

/* STAGING - group_log */
drop table if exists VVJAKELYANDEXRU__STAGING.group_log;
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



/* DWH (DDS) Создание таблицы связи - l_user_group_activity

hk_l_user_group_activity — основной ключ типа INT;
hk_user_id — внешний ключ типа INT, который связан с основным ключом хаба MY__DWH.h_users;
hk_group_id — внешний ключ типа INT, который связан с основным ключом хаба MY__DWH.h_groups;
load_dt — временная отметка типа DATETIME, когда были загружены данные;
load_src — данные об источнике типа VARCHAR(20)
*/
drop table if exists VVJAKELYANDEXRU__DWH.l_user_group_activity;
create table VVJAKELYANDEXRU__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_id_users REFERENCES VVJAKELYANDEXRU__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_group_id_groups REFERENCES VVJAKELYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hash(hk_user_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

/* DWH (DDS) Создание сателлит - s_auth_history 
hk_l_user_group_activity — внешний ключ к ранее созданной таблице связей типа INT. Напомним, в сателлитах нет первичных ключей.
user_id_from — идентификатор того пользователя, который добавил в группу другого. Если новый участник вступил в сообщество сам, это поле пустое. Задайте атрибуту user_id_from тип INT и наполните его из исходных загруженных данных *__STAGING.group_log.
event — событие пользователя в группе;
event_dt — дата и время, когда совершилось событие;
load_dt и load_src — знакомые вам технические поля.
 */
drop table if exists VVJAKELYANDEXRU__DWH.s_auth_history;
create table VVJAKELYANDEXRU__DWH.s_auth_history
(
hk_l_user_group_activity bigint,
user_id_from integer, 
event varchar(6),
event_dt timestamp(6),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hash(user_id_from) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

/* Создание витрины dwh.dm_user_conversion
 * */
drop table if exists VVJAKELYANDEXRU__DWH.dm_user_conversion;
create table VVJAKELYANDEXRU__DWH.dm_user_conversion
(
hk_group_id bigint,
cnt_added_users integer not null, 
cnt_users_in_group_with_messages integer not null,
group_conversion numeric(18,9) not null,
load_dt datetime not null default now()
)
order by group_conversion
SEGMENTED BY hash(load_dt) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
