from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class DataObject:
    """
    Объект-обёртка для передаваемых данных.

    Attributes:
        rows: Список строк данных (например, list[dict]).
        metadata: Дополнительная мета-информация (источник, дата загрузки и т.д.).
    """
    rows: List[Dict[str, Any]]
    metadata: Dict[str, Any]