from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class ProcessResult:
    """
    Общая структура для результатов обработки.

    Attributes:
        status: Строка состояния ('ok', 'error', ...)
        payload: Произвольные результаты обработки (dict, list и т.д.)
        metadata: Дополнительная информация о результате (например, время выполнения).
    """
    status: str
    payload: Any
    metadata: Dict[str, Any]