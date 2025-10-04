--------------------------------------------------------------------------------------------------------------------------------
--Написать процедуру, подсчитывающую количество таблиц, имена которых начинаются с DS% либо REF%. Вывести сообщение с полученными расчётами
--PostgreSQL
--------------------------------------------------------------------------------------------------------------------------------

create or replace function fnc_count_tables_with_prefix()
returns text
language plpgsql
as $$
declare

ds_count integer := 0;
ref_count integer := 0;
total_count integer := 0;
result_message text;

begin
    -- DS таблицы
    select count(*)
    into ds_count
    from information_schema.tables
    where table_schema = 'public'
    and table_name like 'DS%';
    
    -- REF таблицы
    select count(*)
    into ref_count
    from information_schema.tables
    where table_schema = 'public'
    and table_name like 'REF%';
    
    -- Общее количество таблиц
    total_count := ds_count + ref_count;
    
    -- Формирование сообщения с результатами
    result_message := 'Результаты подсчета таблиц:' || E'\n' ||
                     'Таблицы с префиксом DS: ' || ds_count || E'\n' ||
                     'Таблицы с префиксом REF: ' || ref_count || E'\n' ||
                     'Общее количество: ' || total_count;
    
    -- Вывод сообщения
    raise notice '%', result_message;
    
    return result_message;
end;
$$;

