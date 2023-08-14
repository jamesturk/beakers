"""
Internal pydantic models.
"""
import datetime
from enum import Enum
from typing import Callable
from pydantic import BaseModel, ConfigDict


class EdgeType(Enum):
    """
    EdgeType affects how the edge function is processed.

    transform: the output of the edge function is added to the to_beaker
    conditional: if the output of the edge function is truthy, it is added to the to_beaker
    """

    transform = "transform"
    conditional = "conditional"


class Edge(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    func: Callable
    error_map: dict[tuple, str]
    edge_type: EdgeType
    whole_record: bool


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
