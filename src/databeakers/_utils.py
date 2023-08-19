import inspect
from typing import Callable, Iterable
from pydantic import BaseModel


def callable_name(c: Callable) -> str:
    if hasattr(c, "__name__"):
        return "λ" if c.__name__ == "<lambda>" else c.__name__
    else:
        return repr(c)


def required_parameters(func):
    """
    Returns a list of required parameters for a function (sorted).
    """
    parameters = inspect.signature(func).parameters
    return sorted(
        [
            name
            for name, param in parameters.items()
            if param.default is inspect.Parameter.empty
        ]
    )


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
