#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тесты для CSVDataProcessor 
"""

import unittest
import pandas as pd
import numpy as np
from csv_data_processor import CSVDataProcessor 


class TestCSVDataProcessor (unittest.TestCase):
    """Тесты для класса CSVDataProcessor """
    
    def setUp(self):
        """Настройка тестов."""
        self.processor = CSVDataProcessor ()
    
    def test_normalize_phone_number(self):
        """Тест нормализации телефонных номеров."""
        # Создаем Series с тестовыми номерами
        test_phones = pd.Series([
            "1.1.375291234567",
            "2.1.375291234567",
            "80291234567",
            "375 29 123 45 67",
            "+375-29-123-45-67",
            "291234567",
            "",
            None
        ])
        
        result = self.processor.normalize_phone_number(test_phones)
        
        # Проверяем результаты
        expected = pd.Series([
            "375291234567",
            "375291234567",
            "375291234567",
            "375291234567",
            "375291234567",
            "375291234567",
            "",
            ""
        ])
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_convert_time_to_local(self):
        """Тест преобразования времени."""
        # Создаем тестовые Series
        call_dates = pd.Series([
            "10:30:45 15/12/2024",
            "10:30:45 15/12/2024",
            "10:30:45 15/12/2024"
        ])
        
        timezone_offsets = pd.Series([
            "+03:00",
            "-02:00",
            "+00:00"
        ])
        
        result = self.processor.convert_time_to_local(call_dates, timezone_offsets)
        
        # Проверяем результаты
        expected = pd.Series([
            "2024-12-15 13:30:45",
            "2024-12-15 08:30:45",
            "2024-12-15 10:30:45"
        ])
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_determine_call_type(self):
        """Тест определения типа вызова."""
        # Создаем тестовый DataFrame
        test_data = {
            'callDuration': ['120', '', '', '', ''],
            'calledPartyNumber': ['375291234567', '', '375291234567', '', '375291234567'],
            'callingPartyNumber': ['', '375291234567', '', '375291234567', ''],
            'totalVolume': ['', '', '', '', '1024'],
            'totalQuantity': ['', '', '', '1', '']
        }
        
        df = pd.DataFrame(test_data)
        
        result = self.processor.determine_call_type(df)
        
        # Проверяем результаты
        expected = pd.Series([1, 2, 3, 4, 5])  # Исходящий звонок, Входящий звонок, Исходящая SMS, Входящая SMS, Интернет
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_transform_dataframe(self):
        """Тест трансформации DataFrame."""
        # Создаем тестовый DataFrame
        input_data = {
            'partyMSISDN': ['1.1.375291234567'],
            'partyIMSI': ['257012345678901'],
            'calledPartyNumber': ['375291234568'],
            'callingPartyNumber': [''],
            'callDate': ['10:30:45 15/12/2024'],
            'timeZoneOffset': ['+03:00'],
            'callDuration': ['120'],
            'totalVolume': [''],
            'totalQuantity': ['']
        }
        
        input_df = pd.DataFrame(input_data)
        
        result = self.processor.transform_dataframe(input_df)
        
        # Проверяем, что все нужные колонки присутствуют
        expected_columns = [
            'party_msisdn', 'party_imsi', 'called_party_number',
            'calling_party_number', 'call_date', 'call_duration',
            'total_volume', 'total_quantity', 'call_type'
        ]
        
        for col in expected_columns:
            self.assertIn(col, result.columns)
        
        # Проверяем нормализацию номеров
        self.assertEqual(result['party_msisdn'].iloc[0], '375291234567')
        self.assertEqual(result['called_party_number'].iloc[0], '375291234568')
        
        # Проверяем преобразование времени
        self.assertEqual(result['call_date'].iloc[0], '2024-12-15 13:30:45')
        
        # Проверяем тип вызова
        self.assertEqual(result['call_type'].iloc[0], '1')
    
    def test_process_data_empty_file(self):
        """Тест обработки пустого файла."""
        # Создаем пустой DataFrame
        empty_df = pd.DataFrame()
        
        # Мокаем метод read_csv_file
        original_read = self.processor.read_csv_file
        self.processor.read_csv_file = lambda x: empty_df
        
        result = self.processor.process_data("test_file.csv")
        
        # Восстанавливаем оригинальный метод
        self.processor.read_csv_file = original_read
        
        # Проверяем, что возвращается пустой DataFrame
        self.assertTrue(result.empty)
    
    def test_save_to_csv_empty_dataframe(self):
        """Тест сохранения пустого DataFrame."""
        empty_df = pd.DataFrame()
        result = self.processor.save_to_csv(empty_df, "/tmp")
        
        # Проверяем, что возвращается пустая строка
        self.assertEqual(result, "")


if __name__ == '__main__':
    unittest.main()
