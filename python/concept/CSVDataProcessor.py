from typing import Optional
from .BaseProcessor import BaseProcessor
from .DataObject import DataObject

class CSVDataProcessor(BaseProcessor):
    """
    Процессор для выгрузки данных из CSV и загрузки в БД.

    Constructor args:
        default_encoding: кодировка по умолчанию для чтения CSV (str)
        preview_rows: сколько строк возвращать при "preview" (int)
    """

    def __init__(self, default_encoding: str = "utf-8", preview_rows: int = 10):
        self.default_encoding = default_encoding
        self.preview_rows = preview_rows
        print(f"[CSVDataProcessor.__init__] encoding={default_encoding}, preview_rows={preview_rows}")

    def extract_data(self, source: str, delimiter: str = ",", encoding: Optional[str] = None, **options) -> DataObject:
        """
        Выгрузка данных из CSV-файла.

        Args:
            source: путь к CSV-файлу.
            delimiter: разделитель колонок (по умолчанию ',').
            encoding: кодировка (если None — используется default_encoding).
            options: дополнительные параметры (header=True/False, skip_rows=..., и т.д.)

        Returns:
            DataObject со списком строк (list[dict]) и metadata.
        """
        used_encoding = encoding or self.default_encoding
        print(f"[CSVDataProcessor.extract_data] Чтение CSV: {source}, delimiter='{delimiter}', encoding='{used_encoding}'")

        rows = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": None},
        ]
        metadata = {"source": source, "format": "csv", "encoding": used_encoding}
        return DataObject(rows=rows, metadata=metadata)

    def load_data(self, data: DataObject, destination: str, table_name: Optional[str] = None, **options) -> bool:
        """
        Загрузка данных в базу из CSV-прослойки.

        Args:
            data: DataObject с данными для загрузки.
            destination: connection string / идентификатор БД
            table_name: имя таблицы для записи (если None — используется 'default_table')
            if_exists: поведение при существующей таблице: 'append'/'replace'
            options: дополнительные опции (batch_size=1000)

        Returns:
            bool — успех/провал
        """
        table = table_name or "default_table"
        if_exists = options.get("if_exists", "append")
        print(f"[CSVDataProcessor.load_data] Загрузка данных в БД: destination={destination}, table={table}, if_exists={if_exists}")
        print(f"[CSVDataProcessor.load_data] rows_count={len(data.rows)}, metadata={data.metadata}")
        return True