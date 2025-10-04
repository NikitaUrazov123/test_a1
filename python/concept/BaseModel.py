from abc import ABC, abstractmethod
from .DataObject import DataObject
from .ProcessResult import ProcessResult

class BaseModel(ABC):
    """
    Базовый класс модели обработки данных.

    Методы:
        * process_data(data: DataObject, **options) -> ProcessResult
            Обработать входные данные и вернуть результат обработки
        * save_result(result: ProcessResult, destination: str, **options) -> bool
            Сохранить результат обработки в целевую систему (БД, файл и т.д.)
    """

    @abstractmethod
    def process_data(self, data: DataObject, **options) -> ProcessResult:
        """
        Обработать данные

        Args:
            data: DataObject с исходными данными
            options: дополнительные опции для модели (thresholds, rules и т.д.)

        Returns:
            ProcessResult с результатом обработки
        """
        raise NotImplementedError

    @abstractmethod
    def save_result(self, result: ProcessResult, destination: str, **options) -> bool:
        """
        Сохранить результат обработки

        Args:
            result: ProcessResult, который нужно сохранить
            destination: идентификатор места сохранения (например, connection string + table).
            options: дополнительные опции (format='json'|'db', compress=True).

        Returns:
            bool — True при успехе
        """
        raise NotImplementedError