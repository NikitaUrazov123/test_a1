import sys
import os
from csv_data_processor import CSVDataProcessor 


def main():
    input_file = "/home/nik/test_a1/Files/usage_data.log"
    output_dir = "/home/nik/test_a1/python/practice/processed_usage"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    
    print(f"Входной файл: {input_file}")
    print(f"Выходная директория: {output_dir}")
    
    if not os.path.exists(input_file):
        print(f"Ошибка: файл {input_file} не найден")
        print("Использование: python run_processor_ .py [input_file] [output_dir]")
        return 1
    
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Создаем процессор и обрабатываем данные
        processor = CSVDataProcessor ()
        processed_df = processor.process_data(input_file)
        
        if not processed_df.empty:
            output_file = processor.save_to_csv(processed_df, output_dir)
            if output_file:
                print(f"\nОбработка завершена успешно!")
                print(f"Результат сохранен в: {output_file}")
                
                print(f"\nКраткая статистика:")
                print(f"- Обработано записей: {len(processed_df)}")
                print(f"- Типы вызовов: {processed_df['call_type'].value_counts().to_dict()}")
                
                return 0
            else:
                print("Ошибка при сохранении результата")
                return 1
        else:
            print("Не удалось обработать данные")
            return 1
            
    except Exception as e:
        print(f"Ошибка при обработке: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
