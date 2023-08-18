import datetime
from typing import Iterable
from pydantic import BaseModel


class SeedRun(BaseModel):
    run_repr: str
    seed_name: str
    beaker_name: str
    num_items: int
    start_time: datetime.datetime
    end_time: datetime.datetime

    def __str__(self):
        duration = self.end_time - self.start_time
        return (
            f"SeedRun({self.run_repr}, seed_name={self.seed_name}, beaker_name={self.beaker_name}, "
            f"num_items={self.num_items}, duration={duration})"
        )


def _pydantic_to_schema(pydantic_model: type[BaseModel]) -> dict:
    schema = {}
    for k, field in pydantic_model.model_fields.items():
        schema[k] = field.annotation
    return schema


def _pyd_wrap(
    iterable: Iterable[dict], pydantic_model: type[BaseModel]
) -> Iterable[BaseModel]:
    for item in iterable:
        yield pydantic_model(**item)
