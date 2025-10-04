import pandas as pd
import numpy as np
import re
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class CSVDataProcessor :
    
    def __init__(self):
        self.call_type_map = {
            1: "Исходящий звонок",
            2: "Входящий звонок", 
            3: "Исходящая SMS",
            4: "Входящая SMS",
            5: "Интернет"
        }
        
        # Маппинг исходных колонок на целевые
        self.column_mapping = {
            'partyMSISDN': 'party_msisdn',
            'partyIMSI': 'party_imsi', 
            'calledPartyNumber': 'called_party_number',
            'callingPartyNumber': 'calling_party_number',
            'callDate': 'call_date',
            'callDuration': 'call_duration',
            'totalVolume': 'total_volume',
            'totalQuantity': 'total_quantity'
        }
        
        self.processed_records = 0
        self.error_count = 0
        self.stats = {
            'total_calls': 0,
            'total_call_duration': 0,
            'total_volume': 0,
            'total_sms': 0
        }

    def read_csv_file(self, file_path: str) -> pd.DataFrame:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                first_line = file.readline().strip()
                delimiter = ';' if ';' in first_line else ','
            
            df = pd.read_csv(file_path, delimiter=delimiter, dtype=str, na_filter=False)
            
            df = df.fillna('')
            
            return df
                    
        except Exception as e:
            print(f"Ошибка при чтении файла {file_path}: {e}")
            self.error_count += 1
            return pd.DataFrame()

    def normalize_phone_number(self, phone_series: pd.Series) -> pd.Series:
        """
        Нормализует телефонные номера: убирает префиксы, очищает от пробелов и добавляет код 375 для белорусских номеров.
        
        Args:
            phone_series: Series с номерами телефонов
            
        Returns:
            Series с нормализованными номерами
        """
        normalized = phone_series.copy()
        
        normalized = normalized.str.replace(r'^\d+\.\d+\.', '', regex=True)
        
        normalized = normalized.str.replace(r'[^\d]', '', regex=True)
        
        def normalize_single_number(phone):
            if pd.isna(phone) or phone == '':
                return ''
            
            if phone.startswith('375'):
                return phone
                
            if phone.startswith('80') and len(phone) >= 11:
                return '375' + phone[2:]
                
            if len(phone) >= 9 and not phone.startswith('375'):
                return '375' + phone
                
            return phone
        
        return normalized.apply(normalize_single_number)

    def convert_time_to_local(self, call_date_series: pd.Series, timezone_offset_series: pd.Series) -> pd.Series:
        """
        Преобразует время соединения к местному часовому поясу в формате ISO YYYY-MM-DD HH24:MI:SS.
        
        Args:
            call_date_series: Series с датами в формате "HH:MM:SS DD/MM/YYYY"
            timezone_offset_series: Series со смещениями в формате "+03:00"
            
        Returns:
            Series с датами в формате ISO
        """
        def convert_single_time(call_date, timezone_offset):
            try:
                if pd.isna(call_date) or call_date == '' or pd.isna(timezone_offset) or timezone_offset == '':
                    return call_date
                
                time_part, date_part = call_date.split(' ')
                time_components = time_part.split(':')
                date_components = date_part.split('/')
                
                hour, minute, second = map(int, time_components)
                day, month, year = map(int, date_components)
                
                dt = datetime(year, month, day, hour, minute, second)
                
                offset_sign = timezone_offset[0]
                offset_hours = int(timezone_offset[1:3])
                offset_minutes = int(timezone_offset[4:6])
                
                if offset_sign == '+':
                    dt += timedelta(hours=offset_hours, minutes=offset_minutes)
                else:
                    dt -= timedelta(hours=offset_hours, minutes=offset_minutes)
                
                return dt.strftime('%Y-%m-%d %H:%M:%S')
                
            except Exception as e:
                print(f"Ошибка при преобразовании времени '{call_date}': {e}")
                self.error_count += 1
                return call_date
        
        return pd.Series([
            convert_single_time(call_date, timezone_offset) 
            for call_date, timezone_offset in zip(call_date_series, timezone_offset_series)
        ])

    def determine_call_type(self, df: pd.DataFrame) -> pd.Series:
        """
        Определяет тип вызова на основе заполненных полей
        
        Args:
            df: DataFrame с данными записей
            
        Returns:
            Series с кодами типов вызова (1-5)
        """
        has_duration = df['callDuration'].astype(str).str.strip() != ''
        has_volume = df['totalVolume'].astype(str).str.strip() != ''
        has_quantity = df['totalQuantity'].astype(str).str.strip() != ''
        has_called_party = df['calledPartyNumber'].astype(str).str.strip() != ''
        has_calling_party = df['callingPartyNumber'].astype(str).str.strip() != ''
        
        call_types = pd.Series([5] * len(df), dtype=int)  # По умолчанию интернет
        
        # Если заполнена длительность - это звонок
        call_types[has_duration] = np.where(
            has_called_party[has_duration], 1,  # Исходящий звонок
            np.where(has_calling_party[has_duration], 2, 1)  # Входящий или по умолчанию исходящий
        )
        
        # Если заполнен объем - это интернет (уже установлено по умолчанию)
        call_types[has_volume & ~has_duration] = 5
        
        # Если заполнено количество - это SMS
        call_types[has_quantity & ~has_duration & ~has_volume] = np.where(
            has_called_party[has_quantity & ~has_duration & ~has_volume], 3,  # Исходящая SMS
            np.where(has_calling_party[has_quantity & ~has_duration & ~has_volume], 4, 3)  # Входящая или по умолчанию исходящая
        )
        
        # Если ничего не заполнено, определяем по номерам
        no_data = ~has_duration & ~has_volume & ~has_quantity
        call_types[no_data & has_called_party] = 1  # Исходящий звонок
        call_types[no_data & has_calling_party & ~has_called_party] = 2  # Входящий звонок
        
        return call_types

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Выполняет трансформацию DataFrame.
        
        Args:
            df: Исходный DataFrame
            
        Returns:
            Трансформированный DataFrame
        """
        try:
            transformed_df = df.copy()
            
            transformed_df = transformed_df.rename(columns=self.column_mapping)
            
            for phone_field in ['party_msisdn', 'called_party_number', 'calling_party_number']:
                if phone_field in transformed_df.columns:
                    transformed_df[phone_field] = self.normalize_phone_number(transformed_df[phone_field])
            
            if 'call_date' in transformed_df.columns and 'timeZoneOffset' in df.columns:
                transformed_df['call_date'] = self.convert_time_to_local(
                    transformed_df['call_date'], 
                    df['timeZoneOffset']
                )
            
            call_types = self.determine_call_type(df)
            transformed_df['call_type'] = call_types.astype(str)
            
            # Обновляем статистику
            self._update_stats(df, call_types)
            
            # Выбираем только нужные колонки
            target_columns = [
                'party_msisdn', 'party_imsi', 'called_party_number', 
                'calling_party_number', 'call_date', 'call_duration',
                'total_volume', 'total_quantity', 'call_type'
            ]
            
            # Оставляем только существующие колонки
            existing_columns = [col for col in target_columns if col in transformed_df.columns]
            result_df = transformed_df[existing_columns].copy()
            
            return result_df
            
        except Exception as e:
            print(f"Ошибка при трансформации DataFrame: {e}")
            self.error_count += 1
            return pd.DataFrame()

    def _update_stats(self, df: pd.DataFrame, call_types: pd.Series):
        """Обновляет статистику обработки."""
        try:
            # Статистика звонков
            call_mask = call_types.isin([1, 2])
            if call_mask.any():
                call_durations = pd.to_numeric(df.loc[call_mask, 'callDuration'], errors='coerce').fillna(0)
                self.stats['total_calls'] += call_mask.sum()
                self.stats['total_call_duration'] += call_durations.sum()
            
            # Статистика интернета
            internet_mask = call_types == 5
            if internet_mask.any():
                volumes = pd.to_numeric(df.loc[internet_mask, 'totalVolume'], errors='coerce').fillna(0)
                self.stats['total_volume'] += volumes.sum()
            
            # Статистика SMS
            sms_mask = call_types.isin([3, 4])
            if sms_mask.any():
                quantities = pd.to_numeric(df.loc[sms_mask, 'totalQuantity'], errors='coerce').fillna(0)
                self.stats['total_sms'] += quantities.sum()
                
        except Exception as e:
            print(f"Ошибка при обновлении статистики: {e}")

    def process_data(self, input_file: str) -> pd.DataFrame:
        """
        Основной метод обработки данных.
        
        Args:
            input_file: Путь к входному CSV файлу
            
        Returns:
            DataFrame с обработанными данными
        """
        print(f"Начинаем обработку файла: {input_file}")
        start_time = datetime.now()
        
        # Читаем данные
        df = self.read_csv_file(input_file)
        if df.empty:
            print("Не удалось прочитать данные из файла")
            return pd.DataFrame()
            
        print(f"Прочитано записей: {len(df)}")
        
        # Обрабатываем данные
        processed_df = self.transform_dataframe(df)
        
        self.processed_records = len(processed_df)
        end_time = datetime.now()
        
        print(f"Обработка завершена за {end_time - start_time}")
        return processed_df

    def save_to_csv(self, df: pd.DataFrame, output_dir: str) -> str:
        """
        Сохраняет обработанные данные в новый CSV файл.
        
        Args:
            df: DataFrame с обработанными данными
            output_dir: Директория для сохранения
            
        Returns:
            Путь к созданному файлу
        """
        if df.empty:
            return ""
            
        # Создаем имя файла с временной меткой
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"processed_usage_data_ _{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        try:
            # Сохраняем с  
            df.to_csv(filepath, index=False, sep=';', encoding='utf-8')
            
            print(f"Данные сохранены в файл: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"Ошибка при сохранении файла: {e}")
            self.error_count += 1
            return ""

    def print_statistics(self, input_filename: str, output_filename: str, 
                        start_time: datetime, end_time: datetime):
        """
        Выводит статистику обработки.
        
        Args:
            input_filename: Имя входного файла
            output_filename: Имя выходного файла
            start_time: Время начала обработки
            end_time: Время завершения обработки
        """
        print("\n" + "="*60)
        print("СТАТИСТИКА ОБРАБОТКИ ( )")
        print("="*60)
        print(f"Исходный файл: {input_filename}")
        print(f"Результирующий файл: {output_filename}")
        print(f"Общее количество соединений: {self.processed_records}")
        print(f"Время начала обработки: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Время завершения обработки: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Время обработки: {end_time - start_time}")
        print()
        
        print("СВОДКА ПО ТИПАМ СОЕДИНЕНИЙ:")
        print(f"Общее количество звонков: {self.stats['total_calls']}")
        print(f"Суммарная длительность звонков: {self.stats['total_call_duration']} сек")
        print(f"Объем интернет-сессий: {self.stats['total_volume']} байт")
        print(f"Количество SMS: {self.stats['total_sms']}")
        print()
        
        print(f"Количество ошибок при обработке: {self.error_count}")
        print("="*60)


def main():
    """Основная функция программы."""
    # Пути к файлам
    input_file = "/home/nik/test_a1/Files/usage_data.log"
    output_dir = "/home/nik/test_a1/python/practice/processed_usage"
    
    # Проверяем существование входного файла
    if not os.path.exists(input_file):
        print(f"Ошибка: файл {input_file} не найден")
        return
        
    # Создаем выходную директорию если не существует
    os.makedirs(output_dir, exist_ok=True)
    
    # Создаем процессор и обрабатываем данные
    processor = CSVDataProcessor ()
    start_time = datetime.now()
    
    processed_df = processor.process_data(input_file)
    
    if not processed_df.empty:
        output_file = processor.save_to_csv(processed_df, output_dir)
        end_time = datetime.now()
        
        # Выводим статистику
        processor.print_statistics(
            os.path.basename(input_file),
            os.path.basename(output_file),
            start_time,
            end_time
        )
    else:
        print("Не удалось обработать данные")


if __name__ == "__main__":
    main()
