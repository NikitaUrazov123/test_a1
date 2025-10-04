-------------------------------------------------------------------------------------------------
/*
Написать запрос в таблицы USAGE_DATA_HISTORY и USAGE_FILE_HISTORY, собирающий статистику по файлам за предыдущий день:
    * Число уникальных абонентов в этом файле.
    * Общее количество всех событий.
    * Общее количество звонков.
    * Общая длительность звонков в минутах.
    * Общее число SMS.
    * Общий объём всех интернет сессий в мегабайтах.
    * Сумма всех списаний без скидки.
    * Сумма всех списаний со скидкой.
*/
-------------------------------------------------------------------------------------------------

select 
    ufh.file_id
    ,ufh.file_name
    ,ufh.file_date
    ,count(distinct udh.party_id) as distinct_subscribers
    ,count(*) as total_events
    ,count(case 
            when udh.service_id = 1 then 1 
        end) as total_calls
    ,coalesce(
        sum(
            case 
                when udh.service_id = 1 then udh.call_duration 
            end), 0) / 60.0 as total_call_duration_minutes
    ,count(case 
            when udh.service_id = 2 then 1 
            end) as total_sms
    ,coalesce(
        sum(
            case when udh.service_id = 3 then udh.total_volume 
            end), 0) / (1024 * 1024) as total_internet_volume_mib
    ,sum(udh.charge_amount) as total_charges_without_discount
    ,sum(udh.charge_amount - udh.discount_amount) as total_charges_with_discount
from usage_file_history ufh
    left join usage_data_history udh 
        on ufh.file_id = udh.file_id
where date(ufh.file_date) = current_date - interval '1 day'
group by 1,2,3
order by 3 desc;