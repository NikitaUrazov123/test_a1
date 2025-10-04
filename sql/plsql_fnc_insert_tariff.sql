--------------------------------------------------------------------------------------------------------------------------------
/* Написать процедуру, которая выполняет вставку данных в таблицу REF_TARIFF, а также записывает информацию об этой операции в таблицу OPERATION_LOG
Процедура должна:
1. Принимать в качестве аргументов значения для вставки: rate_plan_id, monthly_fee, call_tariff, sms_tariff, internet_tariff, tariff_date.
2. Добавлять запись в таблицу OPERATION_LOG в начале процедуры и обновлять её в конце всей необходимой информацией.
3. Выполнять вставку полученных в аргументах значений в таблицу REF_TARIFF.
*/
-- Oracle 19c
-- сгенерировано gpt на основе pgsql_fnc_insert_tariff.sql, нет возможности проверить работоспособность
--------------------------------------------------------------------------------------------------------------------------------

create or replace function insert_tariff_with_logging(
    p_rate_plan_id in number,
    p_monthly_fee in number,
    p_call_tariff in number,
    p_sms_tariff in number,
    p_internet_tariff in number,
    p_tariff_date in date
)
return varchar2
is
    v_log_id number;
    v_start_time timestamp;
    v_end_time timestamp;
    v_message varchar2(4000);
    v_result varchar2(4000);
begin
    -- Время старта
    v_start_time := systimestamp;
    
    -- Вставка записи в OPERATION_LOG
    insert into operation_log (
        operation_type,
        table_name,
        start_date,
        status,
        message
    ) values (
        'INSERT',
        'REF_TARIFF',
        v_start_time,
        'P',
        'Начало операции вставки тарифа с ID: ' || p_rate_plan_id
    ) returning id into v_log_id;
    
    begin
        -- Вставка в таблицу REF_TARIFF
        insert into ref_tariff (
            rate_plan_id,
            monthly_fee,
            call_tariff,
            sms_tariff,
            internet_tariff,
            tariff_date
        ) values (
            p_rate_plan_id,
            p_monthly_fee,
            p_call_tariff,
            p_sms_tariff,
            p_internet_tariff,
            p_tariff_date
        );
        
        -- Время завершения
        v_end_time := systimestamp;
        v_message := 'Операция выполнена успешно. Тариф с ID ' || p_rate_plan_id || ' добавлен в таблицу REF_TARIFF';
        v_result := 'SUCCESS: ' || v_message;
        
        -- Обновление записи в OPERATION_LOG
        update operation_log 
        set 
            end_date = v_end_time,
            status = 'D',
            message = v_message
        where id = v_log_id;
        
    exception
        when others then
            -- Время завершения
            v_end_time := systimestamp;
            v_message := 'Ошибка при вставке тарифа: ' || sqlerrm;
            v_result := 'ERROR: ' || v_message;
            
            -- Обновление записи в OPERATION_LOG
            update operation_log 
            set 
                end_date = v_end_time,
                status = 'F',
                message = v_message
            where id = v_log_id;
            
            -- Повторный вызов исключения
            raise;
    end;
    
    -- Возврат результата операции
    return v_result;
end;
/
