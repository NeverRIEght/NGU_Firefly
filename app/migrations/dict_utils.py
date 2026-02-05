from typing import Dict, Any


def get_required_or_key_error(field_name: str, data: Dict[str, Any]) -> Any:
    return data[field_name]


def get_optional_or_key_error(field_name: str, data: Dict[str, Any]) -> Any:
    try:
        value = data[field_name]
        if value is None:
            raise KeyError
    except KeyError:
        raise KeyError(f"Required field '{field_name}' is missing or None.")
    return value


def get_optional_or_default(field_name: str, default_value: Any, data: Dict[str, Any]) -> Any:
    try:
        value = data[field_name]
        if value is None:
            value = default_value
    except KeyError:
        value = default_value
    return value
