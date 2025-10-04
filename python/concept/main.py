
from .CSVDataProcessor import CSVDataProcessor
from .TableDataProcessor import TableDataProcessor
from .TestDataModel import TestDataModel
from .StatisticsDataModel import StatisticsDataModel


def main_demo():
    """
    Демо сценария:
    1. Выгрузка данных из CSV -> DataObject
    2. Загрузка извлечённых данных в БД (TableDataProcessor).
    3. Тестирование данных (TestDataModel) и сохранение результата
    4. Сбор статистики (StatisticsDataModel) и сохранение результата
    """
    csv_path = "data/sample.csv"
    db_conn = "postgresql://user:pass@localhost:5432/postgres"

    # Создать процессор CSV и извлечь данные
    csv_processor = CSVDataProcessor(default_encoding="utf-8", preview_rows=5)
    data_obj = csv_processor.extract_data(source=csv_path, delimiter=",")
    print("--- DataObject получен ---")
    print(data_obj)

    # Загрузить данные в таблицу через TableDataProcessor
    table_processor = TableDataProcessor(connection_string=db_conn, schema="public")
    success_load = table_processor.load_data(data=data_obj, destination=db_conn, table_name="raw_imports", if_exists="append")
    print(f"Загрузка в таблицу завершена: {success_load}")

    # Тест данных и сохранение результата
    test_model = TestDataModel(rules={"no_nulls": True}, fail_on_error=False)
    test_result = test_model.process_data(data_obj, verbose=True)
    saved_test = test_model.save_result(test_result, destination=db_conn, table_name="tests_report")
    print(f"Результат тестирования сохранен: {saved_test}")

    # Сбор статистики и сохранение результата
    stats_model = StatisticsDataModel(aggregations={"count": "count", "mean_value": "mean"})
    stats_result = stats_model.process_data(data_obj, group_by=None)
    saved_stats = stats_model.save_result(stats_result, destination=db_conn, table_name="statistics")
    print(f"Статистика сохранена: {saved_stats}")


if __name__ == "__main__":
    main_demo()
