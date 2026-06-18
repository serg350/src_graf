import json
from typing import Any

from .aini.aini_parser import (
    _resolve_templates,
    _runtime_value,
    build_initial_data,
    parse_aini,
)
from .models import Graph


def empty_execution_input_schema() -> dict[str, Any]:
    """
    Что делает: общий fallback для графов без aINI или с ошибкой разбора aINI.
    Место: общий fallback для графов без aINI или с ошибкой разбора aINI.
    Вход: нет.
    Выход: пустая схема формы запуска с нулем предзаполненных полей.
    """
    return {
        "fields": [],
        "prefilled_count": 0,
    }


def build_execution_input_context(raw_aini: str | None) -> tuple[dict[str, Any], str]:
    """
    Что делает: подготовка данных для UI-контролов запуска графа.
    Место: подготовка данных для UI-контролов запуска графа.
    Вход: сырой текст aINI или None.
    Выход: пара (схема полей, текст ошибки); ошибка не пробрасывается наружу.
    """
    if not raw_aini:
        return empty_execution_input_schema(), ""

    try:
        return build_execution_input_schema(raw_aini), ""
    except ValueError as exc:
        return empty_execution_input_schema(), str(exc)


def _extract_sample_from_aini_parameter(parameter: dict[str, Any]) -> Any:
    """
    Что делает: внутренняя нормализация параметра aINI для определения типа UI-поля.
    Место: внутренняя нормализация параметра aINI для определения типа UI-поля.
    Вход: один параметр из parse_aini.
    Выход: пример значения, по которому выбирается input/select/checkbox/number.
    """
    value = parameter.get("value")
    value_type = parameter.get("value_type")

    if value_type in {"text", "file_ref", "section_ref", "dir_ref"}:
        return value
    if value_type == "bool":
        return bool(value)
    if value_type == "dim" and isinstance(value, dict):
        return value.get("value")
    if value_type == "combobox" and isinstance(value, dict):
        return value.get("default")
    if value_type == "interval" and isinstance(value, dict):
        return value.get("current")
    return value


def _has_execution_initial_value(value: Any) -> bool:
    """
    Что делает: расчет метаданных формы запуска.
    Место: расчет метаданных формы запуска.
    Вход: потенциальное начальное значение поля.
    Выход: True, если значение можно считать осмысленно заданным.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return value != ""
    if isinstance(value, (list, tuple, dict)):
        return len(value) > 0
    return True


def _serialize_execution_input_value(value: Any, input_type: str) -> Any:
    """
    Что делает: подготовка initial_value для JSON-ответа фронтенду.
    Место: подготовка initial_value для JSON-ответа фронтенду.
    Вход: Python-значение и тип UI-поля.
    Выход: JSON-совместимое значение; сложные структуры сериализуются строкой.
    """
    if value is None:
        return None
    if input_type == "checkbox":
        return bool(value)
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _format_execution_input_value(value: Any, parameter: dict[str, Any]) -> str:
    """
    Что делает: человекочитаемая подпись начального значения в интерфейсе.
    Место: человекочитаемая подпись начального значения в интерфейсе.
    Вход: значение и исходный параметр aINI.
    Выход: строка для отображения, включая единицы измерения для dim-параметров.
    """
    if value is None:
        return ""

    if isinstance(value, (list, tuple, dict)):
        rendered = json.dumps(value, ensure_ascii=False)
    else:
        rendered = str(value)

    if parameter.get("value_type") in {"dim", "interval"} and isinstance(parameter.get("value"), dict):
        unit = parameter["value"].get("unit", "")
        if unit:
            return f"{rendered} [{unit}]"
    return rendered


def build_execution_input_schema(raw_aini: str) -> dict[str, Any]:
    """
    Что делает: построение схемы формы запуска графа из aINI.
    Место: построение схемы формы запуска графа из aINI.
    Вход: сырой текст aINI.
    Выход: словарь со списком полей, типами ввода, ограничениями, опциями и initial values.
    """
    parsed = parse_aini(raw_aini)
    initial_data = build_initial_data(raw_aini)
    fields = []

    for parameter in parsed["parameters"]:
        sample = _extract_sample_from_aini_parameter(parameter)
        input_type = "text"
        if parameter.get("value_type") == "bool" or isinstance(sample, bool):
            input_type = "checkbox"
        elif parameter.get("value_type") == "combobox":
            input_type = "select"
        elif isinstance(sample, (int, float)) and not isinstance(sample, bool):
            input_type = "number"

        initial_value = initial_data.get(parameter["name"])
        field = {
            "name": parameter["name"],
            "label": parameter["name"].split("$")[-1],
            "section": parameter.get("section", "Input"),
            "required": bool(parameter.get("required")),
            "optional": bool(parameter.get("optional")),
            "comment": parameter.get("comment", ""),
            "value_type": parameter.get("value_type", "text"),
            "input_type": input_type,
            "sample": sample,
            "initial_value": _serialize_execution_input_value(initial_value, input_type),
            "initial_value_label": _format_execution_input_value(initial_value, parameter),
            "has_initial_value": _has_execution_initial_value(initial_value),
            "min": None,
            "max": None,
            "step": 1 if isinstance(sample, int) and not isinstance(sample, bool) else None,
            "options": [],
        }

        if parameter.get("value_type") == "combobox":
            raw_options = parameter.get("value", {}).get("options", [])
            field["options"] = [str(option) for option in raw_options]
        elif parameter.get("value_type") == "interval" and isinstance(parameter.get("value"), dict):
            field["min"] = parameter["value"].get("min")
            field["max"] = parameter["value"].get("max")
            field["step"] = parameter["value"].get("step")
            field["unit"] = parameter["value"].get("unit", "")
        elif input_type == "number" and field["step"] is None:
            field["step"] = "any"

        fields.append(field)

    return {
        "fields": fields,
        "prefilled_count": sum(1 for field in fields if field["has_initial_value"]),
    }


def _coerce_bool(value: Any, field_name: str) -> bool:
    """
    Что делает: валидация пользовательского ввода перед отправкой в исполнитель графа.
    Место: валидация пользовательского ввода перед отправкой в исполнитель графа.
    Вход: произвольное значение и имя поля для сообщения об ошибке.
    Выход: bool; при нераспознанном значении бросает ValueError.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False

    raise ValueError(f"Invalid boolean value for field '{field_name}'")


def _coerce_execution_value(raw_value: Any, parameter: dict[str, Any]) -> Any:
    """
    Что делает: приведение одного пользовательского значения к типу, ожидаемому функциями графа.
    Место: приведение одного пользовательского значения к типу, ожидаемому функциями графа.
    Вход: сырое значение из HTTP-запроса и параметр aINI.
    Выход: типизированное значение; при ошибке типа или недопустимой опции бросает ValueError.
    """
    field_name = parameter["name"]
    sample = _extract_sample_from_aini_parameter(parameter)
    value_type = parameter.get("value_type")

    if value_type == "bool":
        return _coerce_bool(raw_value, field_name)

    if value_type == "combobox":
        options = [str(option) for option in parameter.get("value", {}).get("options", [])]
        as_text = str(raw_value)
        if options and as_text not in options:
            raise ValueError(f"Value '{as_text}' is not allowed for field '{field_name}'")
        raw_value = as_text

    if isinstance(sample, bool):
        return _coerce_bool(raw_value, field_name)

    if isinstance(sample, int) and not isinstance(sample, bool):
        try:
            return int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid integer value for field '{field_name}'") from exc

    if isinstance(sample, float):
        try:
            return float(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid numeric value for field '{field_name}'") from exc

    return raw_value


def parse_execution_request_data(request) -> dict[str, Any]:
    """
    Что делает: входной адаптер endpoint'а запуска графа.
    Место: входной адаптер endpoint'а запуска графа.
    Вход: Django HttpRequest с JSON-body, POST[data] или обычными POST-полями.
    Выход: словарь пользовательских параметров запуска; при неверном формате бросает ValueError.
    """
    payload: Any = {}

    if request.content_type and "application/json" in request.content_type:
        raw_body = request.body.decode("utf-8") if request.body else "{}"
        try:
            payload = json.loads(raw_body or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("Field data must contain valid JSON") from exc
    elif "data" in request.POST:
        raw_data = request.POST.get("data", "{}") or "{}"
        try:
            payload = {"data": json.loads(raw_data)}
        except json.JSONDecodeError as exc:
            raise ValueError("Field data must contain valid JSON") from exc
    else:
        payload = request.POST.dict()

    if not isinstance(payload, dict):
        raise ValueError("Field data must be a JSON object")

    request_data: Any = payload.get("data", payload)
    if isinstance(request_data, str):
        try:
            request_data = json.loads(request_data or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("Field data must contain valid JSON") from exc

    if not isinstance(request_data, dict):
        raise ValueError("Field data must be a JSON object")

    return request_data


def prepare_execution_initial_data(graph: Graph, request_data: dict[str, Any]) -> dict[str, Any]:
    """
    Что делает: финальная подготовка initial_data для Celery-задачи исполнения.
    Место: финальная подготовка initial_data для Celery-задачи исполнения.
    Вход: модель Graph и словарь данных из запроса.
    Выход: очищенный словарь параметров; проверяет обязательные поля aINI и приводит типы.
    """

    if not graph.raw_aini:
        return dict(request_data)

    parsed = parse_aini(graph.raw_aini)
    parameters = {parameter["name"]: parameter for parameter in parsed["parameters"]}

    result = {
        parameter["name"]: _runtime_value(parameter["value_type"], parameter["value"])
        for parameter in parsed["parameters"]
    }

    for field_name, raw_value in request_data.items():
        if field_name in parameters:
            if raw_value in ("", None):
                continue
            result[field_name] = _coerce_execution_value(raw_value, parameters[field_name])
        else:
            result[field_name] = raw_value

    missing_required = [
        name
        for name, parameter in parameters.items()
        if parameter.get("required") and result.get(name) in ("", None)
    ]

    if missing_required:
        raise ValueError(f"Missing required aINI fields: {', '.join(missing_required)}")

    return _resolve_templates(result)
