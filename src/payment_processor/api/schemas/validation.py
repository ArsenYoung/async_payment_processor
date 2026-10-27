import math

from pydantic import JsonValue


def validate_database_text(value: str) -> str:
    if "\x00" in value:
        raise ValueError("Null characters are not allowed")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("Invalid Unicode string") from error
    return value


def validate_json_value(value: JsonValue) -> None:
    if isinstance(value, str):
        validate_database_text(value)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_database_text(key)
            validate_json_value(item)
    elif isinstance(value, list):
        for item in value:
            validate_json_value(item)
