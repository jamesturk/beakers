"""
Internal pydantic models.
"""
import datetime
from enum import Enum
from pydantic import BaseModel


class RunMode(Enum):
    """
    RunMode affects how the pipeline is run.

    waterfall: beakers are processed one at a time, based on a topological sort of the graph
    river: beakers are processed in parallel, with items flowing downstream
    """

    waterfall = "waterfall"
    river = "river"


class RunReport(BaseModel):
    start_time: datetime.datetime
    end_time: datetime.datetime
    only_beakers: list[str] = []
    run_mode: RunMode
    nodes: dict[str, dict[str, int]] = {}


class ErrorType(BaseModel):
    item: BaseModel
    exception: str
    exc_type: str
