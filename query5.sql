-------------------------------------------------------------------------------------------------
--Написать запрос в таблицу USAGE_DATA_HISTORY, возвращающий все события по абонентам за прошлый день с вычислением доли стоимости каждого события в разрезе абонента.
-------------------------------------------------------------------------------------------------

with 
daily_totals as (
    select 
        party_id
        ,sum(charge_amount - discount_amount) as total_daily_cost
    from usage_data_history
    where date(call_date) = current_date - interval '1 day'
    group by party_id
)

select 
    udh.party_id
    ,udh.other_party_id
    ,udh.service_id
    ,udh.call_date
    ,udh.call_duration
    ,udh.total_volume
    ,udh.charged_date
    ,udh.charge_amount
    ,udh.discount_amount
    ,(udh.charge_amount - udh.discount_amount) as net_charge_amount
    ,dt.total_daily_cost
    ,case 
        when dt.total_daily_cost > 0 
        then round(((udh.charge_amount - udh.discount_amount) / dt.total_daily_cost * 100)::numeric,2)
        else 0 
    end as cost_percentage
    ,case udh.service_id
        when 1 then 'Звонок'
        when 2 then 'SMS'
        when 3 then 'Интернет'
    end as service_type
from usage_data_history udh
    join daily_totals dt 
        on udh.party_id = dt.party_id
    where date(udh.call_date) = current_date - interval '1 day'
order by udh.party_id, udh.call_date;
