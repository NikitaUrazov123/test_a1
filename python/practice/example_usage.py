#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Пример использования CSVDataProcessor  для обработки данных звонков.
"""

from csv_data_processor import CSVDataProcessor 
import pandas as pd
import os


def example_basic_usage():
    """Базовый пример использования процессора."""
    print("=== Базовый пример использования ( ) ===")
    
    # Создаем процессор
    processor = CSVDataProcessor ()
    
    # Путь к файлу с данными
    input_file = "/home/nik/test_a1/Files/usage_data.log"
    output_dir = "/home/nik/test_a1/python/practice/processed_usage"
    
    # Обрабатываем данные
    processed_df = processor.process_data(input_file)
    
    if not processed_df.empty:
        # Сохраняем результат
        output_file = processor.save_to_csv(processed_df, output_dir)
        
        # Выводим статистику (используем текущее время)
        from datetime import datetime
        current_time = datetime.now()
        processor.print_statistics(
            os.path.basename(input_file),
            os.path.basename(output_file),
            current_time,
            current_time
        )
    else:
        print("Не удалось обработать данные")


def example_custom_processing():
    """Пример кастомной обработки данных."""
    print("\n=== Пример кастомной обработки ( ) ===")
    
    processor = CSVDataProcessor ()
    
    # Создаем тестовые данные как DataFrame
    test_data = {
        'partyMSISDN': ['1.1.375291234567', '2.1.375291234567', '375291234567'],
        'partyIMSI': ['257012345678901', '257012345678902', '257012345678903'],
        'calledPartyNumber': ['375291234568', '', ''],
        'callingPartyNumber': ['', '375291234569', ''],
        'callDate': ['10:30:45 15/12/2024', '11:15:30 15/12/2024', '12:00:00 15/12/2024'],
        'timeZoneOffset': ['+03:00', '+03:00', '+03:00'],
        'callDuration': ['120', '90', ''],
        'totalVolume': ['', '', '2048'],
        'totalQuantity': ['', '', '']
    }
    
    test_df = pd.DataFrame(test_data)
    
    # Обрабатываем DataFrame
    processed_df = processor.transform_dataframe(test_df)
    
    # Выводим результаты
    print(f"Обработано записей: {len(processed_df)}")
    print("\nРезультат обработки:")
    print(processed_df.to_string(index=False))


def example_phone_normalization():
    """Пример нормализации телефонных номеров."""
    print("\n=== Пример нормализации номеров ( ) ===")
    
    processor = CSVDataProcessor ()
    
    test_numbers = pd.Series([
        "1.1.375291234567",
        "2.1.375291234567", 
        "80291234567",
        "375 29 123 45 67",
        "+375-29-123-45-67",
        "291234567",
        "",
        None
    ])
    
    normalized = processor.normalize_phone_number(test_numbers)
    
    # Создаем DataFrame для красивого вывода
    result_df = pd.DataFrame({
        'Исходный номер': test_numbers,
        'Нормализованный': normalized
    })
    
    print(result_df.to_string(index=False))


def example_call_type_detection():
    """Пример определения типов вызовов."""
    print("\n=== Пример определения типов вызовов ( ) ===")
    
    processor = CSVDataProcessor ()
    
    test_cases = {
        'callDuration': ['120', '90', '', '', ''],
        'calledPartyNumber': ['375291234567', '', '375291234567', '', ''],
        'callingPartyNumber': ['', '375291234567', '', '375291234567', ''],
        'totalVolume': ['', '', '', '', '2048'],
        'totalQuantity': ['', '', '', '1', '']
    }
    
    test_df = pd.DataFrame(test_cases)
    
    call_types = processor.determine_call_type(test_df)
    
    # Создаем результат с названиями типов
    result_data = {
        'Случай': ['Исходящий звонок', 'Входящий звонок', 'Исходящая SMS', 'Входящая SMS', 'Интернет сессия'],
        'Код типа': call_types.values,
        'Название': [processor.call_type_map.get(ct, 'Неизвестно') for ct in call_types.values]
    }
    
    result_df = pd.DataFrame(result_data)
    print(result_df.to_string(index=False))


def example_dataframe_operations():
    """Пример операций с DataFrame."""
    print("\n=== Пример операций с DataFrame ===")
    
    processor = CSVDataProcessor ()
    
    # Создаем более сложный тестовый набор данных
    test_data = {
        'partyMSISDN': ['1.1.375291234567', '2.1.375291234567', '375291234567', '80291234567'],
        'partyIMSI': ['257012345678901', '257012345678902', '257012345678903', '257012345678904'],
        'calledPartyNumber': ['375291234568', '', '375291234568', ''],
        'callingPartyNumber': ['', '375291234569', '', '375291234569'],
        'callDate': ['10:30:45 15/12/2024', '11:15:30 15/12/2024', '12:00:00 15/12/2024', '13:45:12 15/12/2024'],
        'timeZoneOffset': ['+03:00', '+03:00', '+03:00', '+03:00'],
        'callDuration': ['120', '90', '', ''],
        'totalVolume': ['', '', '2048', '1024'],
        'totalQuantity': ['', '', '', '']
    }
    
    test_df = pd.DataFrame(test_data)
    
    print("Исходные данные:")
    print(test_df.to_string(index=False))
    
    # Обрабатываем данные
    processed_df = processor.transform_dataframe(test_df)
    
    print("\nОбработанные данные:")
    print(processed_df.to_string(index=False))
    
    # Показываем статистику по типам
    print("\nСтатистика по типам вызовов:")
    type_stats = processed_df['call_type'].value_counts().sort_index()
    for call_type, count in type_stats.items():
        type_name = processor.call_type_map.get(int(call_type), 'Неизвестно')
        print(f"Тип {call_type} ({type_name}): {count} записей")


def example_performance_comparison():
    """Пример сравнения производительности."""
    print("\n=== Сравнение производительности ===")
    
    import time
    
    processor = CSVDataProcessor ()
    
    # Создаем большой тестовый набор данных
    n_records = 10000
    
    test_data = {
        'partyMSISDN': [f"1.1.375291234{str(i).zfill(3)}" for i in range(n_records)],
        'partyIMSI': [f"257012345678{str(i).zfill(3)}" for i in range(n_records)],
        'calledPartyNumber': [f"375291234{str(i).zfill(3)}" if i % 2 == 0 else "" for i in range(n_records)],
        'callingPartyNumber': ["" if i % 2 == 0 else f"375291234{str(i).zfill(3)}" for i in range(n_records)],
        'callDate': ["10:30:45 15/12/2024" for _ in range(n_records)],
        'timeZoneOffset': ["+03:00" for _ in range(n_records)],
        'callDuration': [str(120 + i) if i % 3 == 0 else "" for i in range(n_records)],
        'totalVolume': ["" if i % 3 == 0 else str(1024 + i) for i in range(n_records)],
        'totalQuantity': ["" for _ in range(n_records)]
    }
    
    test_df = pd.DataFrame(test_data)
    
    print(f"Тестируем на {n_records} записях...")
    
    start_time = time.time()
    processed_df = processor.transform_dataframe(test_df)
    end_time = time.time()
    
    processing_time = end_time - start_time
    
    print(f"Время обработки: {processing_time:.3f} секунд")
    print(f"Скорость: {n_records / processing_time:.0f} записей/сек")
    print(f"Обработано записей: {len(processed_df)}")


if __name__ == "__main__":
    # Запускаем примеры
    example_basic_usage()
    example_custom_processing()
    example_phone_normalization()
    example_call_type_detection()
    example_dataframe_operations()
    example_performance_comparison()
