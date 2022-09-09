SELECT hk_group_id, cnt_added_users, cnt_users_in_group_with_messages, group_conversion, load_dt
FROM VVJAKELYANDEXRU__DWH.dm_user_conversion
order by group_conversion desc;
