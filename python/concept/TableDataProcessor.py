from typing import Optional
from .BaseProcessor import BaseProcessor
from .DataObject import DataObject

class TableDataProcessor(BaseProcessor):
    """
    Процессор для загрузки и выгрузки данных в табличную базу данных.

    Constructor args:
        connection_string: строка подключения к БД
        schema: схема по умолчанию (optional)
    """

    def __init__(self, connection_string: str, schema: Optional[str] = None):
        self.connection_string = connection_string
        self.schema = schema
        print(f"[TableDataProcessor.__init__] connection_string={connection_string}, schema={schema}")

    def extract_data(self, source: str, table_name: Optional[str] = None, limit: Optional[int] = None, **options) -> DataObject:
        """
        Извлечь данные из таблицы БД

        Args:
            source: идентификатор/connection string (может совпадать с self.connection_string).
            table_name: имя таблицы для чтения.
            limit: ограничение по числу строк (если нужно).

        Returns:
            DataObject c данными из таблицы
        """
        table = table_name or "unknown_table"
        print(f"[TableDataProcessor.extract_data] Чтение из БД: source={source}, table={table}, limit={limit}")
        rows = [
            {"id": "a", "score": 0.9},
            {"id": "b", "score": 0.75},
        ][:limit]
        metadata = {"source": source, "table": table, "fetched_count": len(rows)}
        return DataObject(rows=rows, metadata=metadata)

    def load_data(self, data: DataObject, destination: str, table_name: str, if_exists: str = "append", **options) -> bool:
        """
        Загрузить DataObject в таблицу базы данных

        Args:
            data: DataObject с данными
            destination: connection string / идентификатор БД
            table_name: имя таблицы
            if_exists: поведение при существующей таблице: 'append'|'replace'|'fail'
            options: дополнительные опции (batch_size, transaction=True/False, и т.д.)

        Returns:
            bool — True при успешной загрузке
        """
        print(f"[TableDataProcessor.load_data] Загрузка в таблицу '{table_name}' на {destination} (if_exists={if_exists})")
        print(f"[TableDataProcessor.load_data] rows_count={len(data.rows)}, metadata={data.metadata}")
        return True