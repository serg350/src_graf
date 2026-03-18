from typing import Any, Dict

from .aini_parser import parse_aini as _parse_aini


def parse_aini_schema(raw_aini: str) -> Dict[str, Any]:
    """Backward-compatible alias for schema validation."""
    return _parse_aini(raw_aini)


def parse_aini(raw_aini: str) -> Dict[str, Any]:
    """Backward compatibility for existing imports."""
    return _parse_aini(raw_aini)
