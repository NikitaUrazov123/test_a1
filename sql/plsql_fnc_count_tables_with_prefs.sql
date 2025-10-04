--------------------------------------------------------------------------------------------------------------------------------
--Написать процедуру, подсчитывающую количество таблиц, имена которых начинаются с DS% либо REF%. Вывести сообщение с полученными расчётами
--Oracle 19c
--сгенерировано gpt на основе fnc_count_tables_with_pref.sql, нет возможности проверить работоспособность
--------------------------------------------------------------------------------------------------------------------------------

create or replace function fnc_count_tables_with_prefix
return varchar2
as
    ds_count number := 0;
    ref_count number := 0;
    total_count number := 0;
    result_message varchar2(1000);
begin
    -- DS таблицы
    select count(*)
    into ds_count
    from user_tables
    where table_name like 'DS%';
    
    -- REF таблицы
    select count(*)
    into ref_count
    from user_tables
    where table_name like 'REF%';
    
    -- Общее количество таблиц
    total_count := ds_count + ref_count;
    
    -- Формирование сообщения с результатами
    result_message := 'Результаты подсчета таблиц:' || chr(10) ||
                     'Таблицы с префиксом DS: ' || ds_count || chr(10) ||
                     'Таблицы с префиксом REF: ' || ref_count || chr(10) ||
                     'Общее количество: ' || total_count;
    
    -- Вывод сообщения
    dbms_output.put_line(result_message);
    
    return result_message;
end;
/
