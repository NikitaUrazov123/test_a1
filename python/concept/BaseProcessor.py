from abc import ABC, abstractmethod
from .DataObject import DataObject

class BaseProcessor(ABC):
    """
    Базовый интерфейс для процессоров данных.
    Методы:
    * extract_data(source: str, **options) -> DataObject
        Извлечение данных из указанного источника.
    * load_data(data: DataObject, destination: str, **options) -> bool
        Загрузка данных в указанный приемник.
    """

    @abstractmethod
    def extract_data(self, source: str, **options) -> DataObject:
        """
        Извлечь данные из источника.

        Args:
            source: путь или идентификатор источника (путь к CSV-файлу)
            options: дополнительные опции (encoding, delimiter)

        Returns:
            DataObject — обёртка с данными и metadata.
        """
        raise NotImplementedError

    @abstractmethod
    def load_data(self, data: DataObject, destination: str, **options) -> bool:
        """
        Загрузить данные в целевую систему.

        Args:
            data: объект DataObject с данными.
            destination: идентификатор места загрузки (например, connection string + table)
            options: дополнительные опции (batch_size, upsert)

        Returns:
            bool — True при успехе, False при неудаче.
        """
        raise NotImplementedError