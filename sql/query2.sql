-------------------------------------------------------------------------------------------------
--Написать запрос в таблицу PHONE_CATALOG, выводящий модель телефона с минимальной стоимостью первоначального платежа, который можно купить в рассрочку, есть выбор цвета, страна производства начинается на *C* и с датой поставки позже *15 марта 2024*.
-------------------------------------------------------------------------------------------------
with 
filtered_source as (
    select 
    phone_name
    ,color
    ,country
    ,date_delivery
    ,price
    ,installment
    ,initial_payment
    from phone_catalog
    where installment = 'Y'
    and country like 'C%'
    and date_delivery > '2024-03-15'
)

,props as(
    select 
    *
    ,count(color) over (partition by phone_name) as count_colors_by_model
    ,min(initial_payment) over (partition by phone_name) as min_initial_payment_by_model
    from filtered_source
)

select * from props
where count_colors_by_model > 1
and initial_payment = min_initial_payment_by_model
limit 1;