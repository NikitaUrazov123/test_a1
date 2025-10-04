-------------------------------------------------------------------------------------------------
--  Написать запрос в таблицу USAGE_DATA_HISTORY, возвращающий все события по абонентам за прошлый день, проранжированным по времени события для каждого абонента.
-------------------------------------------------------------------------------------------------

select 
    party_id
    ,other_party_id
    ,service_id
    ,call_date
    ,call_duration
    ,total_volume
    ,charged_date
    ,charge_amount
    ,discount_amount
    ,file_id
    ,row_number() over (partition by party_id order by call_date desc) as event_rank
from usage_data_history
where date(call_date) = date(current_timestamp) - interval '1 day'
order by party_id, call_date;
