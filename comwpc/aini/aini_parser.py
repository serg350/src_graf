import re
from typing import Any, Dict, List, Tuple

_SECTION_RE = re.compile(r"^\[(?P<name>[^\]]+)\]$")
_PARAM_RE = re.compile(r"^(?P<prefix>[*\-]?)(?P<name>[A-Za-z0-9_]+(?:\$[A-Za-z0-9_]+)?)\s*=\s*(?P<value>.+)$")
_BOOL_RE = re.compile(r"^\[(?P<value>[01])\]\{0\|1\}$")
_COMBOBOX_RE = re.compile(r"^\[(?P<current>[^\]]+)\]\{(?P<options>[^}]+)\}$")
_INTERVAL_RE = re.compile(r"^\[(?P<current>[^;\]]+)\s*;\s*(?P<min>[^:\]]+)\s*:\s*(?P<max>[^;\]]+)\s*;\s*(?P<step>[^\]]+)\]$")
_DBTABLE_RE = re.compile(r"^\[(?P<record>[^\]]+)\]\$(?P<schema>[A-Za-z0-9_]+)\.(?P<table>[A-Za-z0-9_]+)$")
_FILE_RE = re.compile(r"^\[(?P<filename>[^\]]+\.[^\]]+)\]$")
_DIM_RE = re.compile(r"^(?P<value>.+?)\s*\[\[(?P<unit>[^\]]+)\]\]$")
_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?$")
_PLACEHOLDER_RE = re.compile(r"@([A-Za-z0-9_]+)@")


def parse_aini(raw_aini: str) -> Dict[str, Any]:
    """
    Что делает: низкоуровневый парсер aINI, используется импортом графов и схемой параметров запуска.
    Место: низкоуровневый парсер aINI, используется импортом графов и схемой параметров запуска.
    Вход: сырой текст aINI.
    Выход: словарь с секциями и плоским списком параметров; при ошибке формата бросает ValueError.
    """
    if not raw_aini or not raw_aini.strip():
        raise ValueError("aINI is empty")

    sections: List[Dict[str, Any]] = []
    current_section: Dict[str, Any] | None = None

    for line_no, line in enumerate(raw_aini.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue

        content, comment = _split_inline_comment(stripped)
        if not content:
            continue

        section_match = _SECTION_RE.match(content)
        if section_match:
            current_section = {"name": section_match.group("name").strip(), "comment": comment, "parameters": []}
            sections.append(current_section)
            continue

        if current_section is None:
            raise ValueError(f"Invalid aINI: parameter before first section at line {line_no}")

        match = _PARAM_RE.match(content)
        if not match:
            raise ValueError(f"Invalid aINI parameter at line {line_no}: {content}")

        value_type, value = _parse_value(match.group("value").strip())
        current_section["parameters"].append({
            "name": match.group("name"),
            "section": current_section["name"],
            "required": match.group("prefix") == "*",
            "optional": match.group("prefix") == "-",
            "value_type": value_type,
            "value": value,
            "comment": comment,
        })

    if not sections:
        raise ValueError("aINI must contain at least one section")

    return {
        "sections": sections,
        "parameters": [p for section in sections for p in section["parameters"]],
    }


def build_initial_data(raw_aini: str) -> Dict[str, Any]:
    """
    Что делает: подготовка дефолтных значений запуска по aINI для формы и истории исполнения.
    Место: подготовка дефолтных значений запуска по aINI для формы и истории исполнения.
    Вход: сырой текст aINI.
    Выход: словарь runtime-значений параметров с разрешенными шаблонами вида @Name@.
    """
    parsed = parse_aini(raw_aini)
    values: Dict[str, Any] = {}
    for section in parsed["sections"]:
        for param in section["parameters"]:
            values[param["name"]] = _runtime_value(param["value_type"], param["value"])
    return _resolve_templates(values)


def _parse_value(raw: str) -> Tuple[str, Any]:
    """
    Что делает: внутренняя часть aINI-парсера, распознает тип значения одного параметра.
    Место: внутренняя часть aINI-парсера, распознает тип значения одного параметра.
    Вход: строковое значение справа от знака равенства.
    Выход: пара (тип значения, разобранное Python-значение).
    """
    if (m := _BOOL_RE.match(raw)):
        return "bool", m.group("value") == "1"
    if (m := _COMBOBOX_RE.match(raw)):
        body = m.group("options")
        delimiter = "|" if "|" in body else ";"
        options = [_parse_scalar(v.strip()) for v in _split_top_level(body, delimiter) if v.strip()]
        return "combobox", {"current": _parse_scalar(m.group("current")), "options": options}
    if (m := _INTERVAL_RE.match(raw)):
        return "interval", {
            "current": _parse_scalar(m.group("current")),
            "min": _parse_scalar(m.group("min")),
            "max": _parse_scalar(m.group("max")),
            "step": _parse_scalar(m.group("step")),
        }
    if (m := _DBTABLE_RE.match(raw)):
        return "dbtable_ref", {
            "record_id": [_parse_scalar(v.strip()) for v in m.group("record").split(";") if v.strip()],
            "schema": m.group("schema"),
            "table": m.group("table"),
        }
    if (m := _FILE_RE.match(raw)):
        return "file_ref", m.group("filename")
    if (m := _DIM_RE.match(raw)):
        return "dim", {"value": _parse_scalar(m.group("value")), "unit": m.group("unit").strip()}
    if raw.startswith("(") and raw.endswith(")"):
        return "array", _parse_array(raw)
    if raw.startswith("{") and raw.endswith("}"):
        return "set", _parse_set(raw)
    return "text", _parse_scalar(raw)


def _runtime_value(value_type: str, value: Any) -> Any:
    """
    Что делает: адаптер aINI-значений к данным, которые реально передаются в исполнитель графа.
    Место: адаптер aINI-значений к данным, которые реально передаются в исполнитель графа.
    Вход: тип параметра и значение из parse_aini.
    Выход: скалярное runtime-значение или исходная структура для сложных типов.
    """
    if value_type in {"dim", "interval", "combobox"}:
        return value["current"] if value_type != "dim" else value["value"]
    return value


def _parse_scalar(value: str) -> Any:
    """
    Что делает: базовая нормализация атомарных значений aINI.
    Место: базовая нормализация атомарных значений aINI.
    Вход: строковый токен без внешней структуры массива/набора.
    Выход: int, float, bool или исходная строка, если числовой/булев тип не распознан.
    """
    value = value.strip()
    if not value:
        return ""
    if _PLACEHOLDER_RE.search(value):
        return value
    if _INT_RE.match(value):
        normalized = value.lstrip("+-")
        if len(normalized) == 1 or not normalized.startswith("0"):
            return int(value)
    if _FLOAT_RE.match(value):
        try:
            return float(value)
        except ValueError:
            pass
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return value


def _parse_set(raw: str) -> List[Any]:
    """
    Что делает: разбор aINI-наборов в фигурных скобках.
    Место: разбор aINI-наборов в фигурных скобках.
    Вход: строка вида {a;b;c}, {a,b,c} или {a|b|c}.
    Выход: список разобранных скалярных значений.
    """
    body = raw[1:-1].strip()
    if not body:
        return []
    delimiter = "|" if "|" in body else "," if "," in body else ";"
    return [_parse_scalar(v.strip()) for v in _split_top_level(body, delimiter) if v.strip()]


def _parse_array(raw: str) -> List[Any]:
    """
    Что делает: разбор aINI-массивов, включая вложенные массивы.
    Место: разбор aINI-массивов, включая вложенные массивы.
    Вход: строка массива в круглых скобках.
    Выход: список скаляров или вложенных списков.
    """
    body = raw[1:-1].strip()
    if not body:
        return []
    result: List[Any] = []
    for token in _split_top_level(body, ";"):
        token = token.strip()
        if not token:
            continue
        if token.startswith("(") and token.endswith(")"):
            result.append(_parse_array(token))
        else:
            result.append(_parse_scalar(token))
    return result


def _resolve_templates(values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Что делает: постобработка начальных данных aINI перед запуском графа.
    Место: постобработка начальных данных aINI перед запуском графа.
    Вход: словарь параметров, где строки могут ссылаться на другие параметры через @Name@.
    Выход: новый словарь с подставленными значениями; исходный словарь не мутируется.
    """
    resolved = dict(values)
    for _ in range(8):
        changed = False
        for key, current in list(resolved.items()):
            if isinstance(current, str):
                new_value = _PLACEHOLDER_RE.sub(lambda m: str(resolved.get(m.group(1), m.group(0))), current)
                if new_value != current:
                    resolved[key] = _parse_scalar(new_value)
                    changed = True
        if not changed:
            break
    return resolved


def _split_inline_comment(line: str) -> Tuple[str, str]:
    """
    Что делает: лексический помощник aINI-парсера.
    Место: лексический помощник aINI-парсера.
    Вход: одна строка aINI.
    Выход: пара (содержимое без комментария, комментарий), не разрезая // внутри кавычек и скобок.
    """
    in_quotes = False
    p_depth = b_depth = c_depth = 0
    for i in range(len(line) - 1):
        ch, nxt = line[i], line[i + 1]
        if ch == '"' and (i == 0 or line[i - 1] != "\\"):
            in_quotes = not in_quotes
        elif not in_quotes:
            if ch == "(":
                p_depth += 1
            elif ch == ")":
                p_depth = max(0, p_depth - 1)
            elif ch == "[":
                b_depth += 1
            elif ch == "]":
                b_depth = max(0, b_depth - 1)
            elif ch == "{":
                c_depth += 1
            elif ch == "}":
                c_depth = max(0, c_depth - 1)
            elif ch == "/" and nxt == "/" and p_depth == 0 and b_depth == 0 and c_depth == 0:
                return line[:i].rstrip(), line[i + 2 :].strip()
    return line.strip(), ""


def _split_top_level(value: str, delimiter: str) -> List[str]:
    """
    Что делает: лексический помощник для списковых значений aINI.
    Место: лексический помощник для списковых значений aINI.
    Вход: строка и разделитель верхнего уровня.
    Выход: список частей, разделенных только вне кавычек и вложенных скобок.
    """
    result: List[str] = []
    current: List[str] = []
    in_quotes = False
    p_depth = b_depth = c_depth = 0
    for ch in value:
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
            continue
        if not in_quotes:
            if ch == "(":
                p_depth += 1
            elif ch == ")":
                p_depth -= 1
            elif ch == "[":
                b_depth += 1
            elif ch == "]":
                b_depth -= 1
            elif ch == "{":
                c_depth += 1
            elif ch == "}":
                c_depth -= 1
            if ch == delimiter and p_depth == 0 and b_depth == 0 and c_depth == 0:
                result.append("".join(current).strip())
                current = []
                continue
        current.append(ch)
    if current:
        result.append("".join(current).strip())
    return result
