
from typing import Optional, Dict, Any
from .BaseModel import BaseModel
from .DataObject import DataObject
from .ProcessResult import ProcessResult

class TestDataModel(BaseModel):
    """
    Модель для тестирования данных.

    Constructor args:
        rules: набор правил/порогов для тестов (dict).
        fail_on_error: если True — бросать исключение при сильной ошибке (в концепте просто логируем).
    """

    def __init__(self, rules: Optional[Dict[str, Any]] = None, fail_on_error: bool = False):
        self.rules = rules or {}
        self.fail_on_error = fail_on_error
        print(f"[TestDataModel.__init__] rules={self.rules}, fail_on_error={self.fail_on_error}")

    def process_data(self, data: DataObject, verbose: bool = True, **options) -> ProcessResult:
        """
        Провести тестирование данных (валидация, проверки целостности).

        Args:
            data: DataObject с данными.
            verbose: печатать подробную информацию о тестах.
            options: дополнительные параметры тестирования.

        Returns:
            ProcessResult где payload — отчет о тестах (например, dict с ошибками/статистикой).
        """
        print(f"[TestDataModel.process_data] Выполняем тесты над данными (rows={len(data.rows)}). verbose={verbose}")
        # Заглушка: собираем фиктивный отчет
        test_report = {
            "total_rows": len(data.rows),
            "null_values": sum(1 for r in data.rows for v in r.values() if v is None),
            "errors": [],
            "passed": True
        }
        metadata = {"model": "TestDataModel", "rules_used": self.rules}
        return ProcessResult(status="ok", payload=test_report, metadata=metadata)

    def save_result(self, result: ProcessResult, destination: str, table_name: Optional[str] = None, **options) -> bool:
        """
        Сохранить результат тестирования в БД.

        Args:
            result: ProcessResult с отчетом.
            destination: connection string к БД.
            table_name: имя таблицы для сохранения результата.
            options: дополнительные параметры (write_mode и т.д.)

        Returns:
            bool — успех сохранения.
        """
        table = table_name or "data_tests"
        print(f"[TestDataModel.save_result] Сохраняем тест-отчет в {destination}, table={table}")
        print(f"[TestDataModel.save_result] result.status={result.status}, payload={result.payload}")
        # Заглушка: симулируем сохранение
        return True