from typing import Any, Dict

from .aini_parser import parse_aini as _parse_aini


def parse_aini_schema(raw_aini: str) -> Dict[str, Any]:
    """
    Что делает: совместимость старых импортов схемы aINI.
    Место: совместимость старых импортов схемы aINI.
    Вход: сырой текст aINI.
    Выход: результат основного parse_aini; при ошибке формата бросает ValueError.
    """
    return _parse_aini(raw_aini)


def parse_aini(raw_aini: str) -> Dict[str, Any]:
    """
    Что делает: совместимость существующих вызовов comwpc.aini.aini_schema.parse_aini.
    Место: совместимость существующих вызовов comwpc.aini.aini_schema.parse_aini.
    Вход: сырой текст aINI.
    Выход: нормализованная структура aINI из основного парсера.
    """
    return _parse_aini(raw_aini)
