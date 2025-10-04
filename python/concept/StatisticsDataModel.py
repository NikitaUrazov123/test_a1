
from typing import Optional, Dict, List
from .BaseModel import BaseModel
from .DataObject import DataObject
from .ProcessResult import ProcessResult

class StatisticsDataModel(BaseModel):
    """
    Модель для вычисления статистики по данным.

    Constructor args:
        aggregations: описание агрегирующих функций (list или dict).
    """

    def __init__(self, aggregations: Optional[Dict[str, str]] = None):
        self.aggregations = aggregations or {"count": "count", "mean": "mean"}
        print(f"[StatisticsDataModel.__init__] aggregations={self.aggregations}")

    def process_data(self, data: DataObject, group_by: Optional[List[str]] = None, **options) -> ProcessResult:
        """
        Собрать статистику по данным.

        Args:
            data: DataObject.
            group_by: список полей для группировки (если необходимо).
            options: дополнительные параметры (agg_funcs, filters).

        Returns:
            ProcessResult: payload содержит агрегированные данные (например, dict).
        """
        print(f"[StatisticsDataModel.process_data] Сбор статистики (rows={len(data.rows)}), group_by={group_by}")
        # Заглушка: делаем простую "агрегацию"
        count = len(data.rows)
        # Попробуем вычислить среднее по полю 'value' если есть
        values = [r.get("value") for r in data.rows if isinstance(r.get("value"), (int, float))]
        mean_value = (sum(values) / len(values)) if values else None
        stats = {"count": count, "mean_value": mean_value}
        metadata = {"model": "StatisticsDataModel", "group_by": group_by}
        return ProcessResult(status="ok", payload=stats, metadata=metadata)

    def save_result(self, result: ProcessResult, destination: str, table_name: Optional[str] = None, **options) -> bool:
        """
        Сохранить статистику в базу данных.

        Args:
            result: ProcessResult со статистикой.
            destination: connection string.
            table_name: имя таблицы.
            options: дополнительные опции.

        Returns:
            bool — True при успехе.
        """
        table = table_name or "data_statistics"
        print(f"[StatisticsDataModel.save_result] Сохраняем статистику в {destination}, table={table}")
        print(f"[StatisticsDataModel.save_result] result.payload={result.payload}")
        # Заглушка: симулируем успешное сохранение
        return True