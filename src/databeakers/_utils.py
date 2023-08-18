from typing import Callable, Iterable
from pydantic import BaseModel


def callable_name(c: Callable) -> str:
    if hasattr(c, "__name__"):
        return "λ" if c.__name__ == "<lambda>" else c.__name__
    else:
        return repr(c)


def pydantic_to_schema(pydantic_model: type[BaseModel]) -> dict:
    schema = {}
    for k, field in pydantic_model.model_fields.items():
        schema[k] = field.annotation
    return schema


def pyd_wrap(
    iterable: Iterable[dict], pydantic_model: type[BaseModel]
) -> Iterable[BaseModel]:
    for item in iterable:
        yield pydantic_model(**item)
