"""
Internal pydantic models.
"""
import datetime
from enum import Enum
from pydantic import BaseModel


class Seed(BaseModel):
    name: str
    num_items: int = 0
    imported_at: str | None = None

    def __str__(self) -> str:
        if self.imported_at:
            return (
                f"{self.name} ({self.num_items} items imported at {self.imported_at})"
            )
        else:
            return f"{self.name}"


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


# not in models because it is used externally
class ErrorType(BaseModel):
    item: BaseModel
    exception: str
    exc_type: str
