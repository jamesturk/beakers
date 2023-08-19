"""
Internal pydantic models.
"""
import datetime
from enum import Enum
from pydantic import BaseModel
from typing import Callable, Iterable
from ._utils import required_parameters


class RunMode(Enum):
    """
    RunMode affects how the pipeline is run.

    waterfall: beakers are processed one at a time, based on a topological sort of the graph
    river: beakers are processed in parallel, with items flowing downstream
    """

    waterfall = "waterfall"
    river = "river"


class RunReport(BaseModel):
    """
    Represents the result of a run.
    """

    start_time: datetime.datetime
    end_time: datetime.datetime
    only_beakers: list[str] = []
    run_mode: RunMode
    nodes: dict[str, dict[str, int]] = {}


class ErrorType(BaseModel):
    """
    Beaker type for errors.
    """

    item: BaseModel
    exception: str
    exc_type: str


class SeedRun(BaseModel):
    """
    Database model for a seed run.
    """

    run_repr: str
    seed_name: str
    beaker_name: str
    num_items: int
    start_time: datetime.datetime
    end_time: datetime.datetime
    error: str

    def __str__(self):
        duration = self.end_time - self.start_time
        return (
            f"SeedRun({self.run_repr}, seed_name={self.seed_name}, beaker_name={self.beaker_name}, "
            f"num_items={self.num_items}, duration={duration}, error={self.error}))"
        )


class Seed(BaseModel):
    """
    Internal representation of a seed.
    """

    name: str
    func: Callable[[], Iterable[BaseModel]]
    beaker_name: str

    @property
    def display_name(self):
        parameters = required_parameters(self.func)
        if parameters:
            return f"{self.name}({', '.join(parameters)})"
        else:
            return self.name
