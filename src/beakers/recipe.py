import inspect
import sqlite3
import asyncio
import datetime
import networkx  # type: ignore
from enum import StrEnum
from collections import defaultdict
from typing import Iterable, Callable, Type
from pydantic import BaseModel, ConfigDict
from structlog import get_logger

from .beakers import Beaker, SqliteBeaker, TempBeaker
from .exceptions import SeedError

log = get_logger()


class EdgeType(StrEnum):
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


class Seed(BaseModel):
    name: str
    num_items: int = 0
    imported_at: str | None = None

    def __str__(self):
        if self.imported_at:
            return (
                f"{self.name} ({self.num_items} items imported at {self.imported_at})"
            )
        else:
            return f"{self.name}"


class RunReport(BaseModel):
    start_time: datetime.datetime
    end_time: datetime.datetime
    start_beaker: str | None
    end_beaker: str | None
    nodes: dict[str, dict[str, int]] = {}


class ErrorType(BaseModel):
    item: BaseModel
    exception: str
    exc_type: str


class Recipe:
    def __init__(self, name: str, db_name: str = "beakers.db"):
        self.name = name
        self.graph = networkx.DiGraph()
        self.beakers: dict[str, Beaker] = {}
        self.seeds: dict[str, tuple[str, Callable[[], Iterable[BaseModel]]]] = {}
        self.db = sqlite3.connect(db_name)
        cursor = self.db.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS _seeds (
                name TEXT, 
                beaker_name TEXT,
                num_items INTEGER,
                imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
        )

    def __repr__(self) -> str:
        return f"Recipe({self.name})"

    # section: graph ##########################################################

    def add_beaker(
        self,
        name: str,
        datatype: Type[BaseModel],
        beaker_type: Type[Beaker] = SqliteBeaker,
    ) -> Beaker:
        self.graph.add_node(name, datatype=datatype)
        self.beakers[name] = beaker_type(name, datatype, self)
        return self.beakers[name]

    def add_transform(
        self,
        from_beaker: str,
        to_beaker: str,
        func: Callable,
        *,
        name: str | None = None,
        edge_type: EdgeType = EdgeType.transform,
        error_map: dict[tuple, str] | None = None,
    ) -> None:
        if name is None:
            name = func.__name__ if func.__name__ != "<lambda>" else "λ"
        edge = Edge(
            name=name,
            edge_type=edge_type,
            func=func,
            error_map=error_map or {},
        )
        self.graph.add_edge(
            from_beaker,
            to_beaker,
            edge=edge,
        )

    # section: seeds ##########################################################

    def add_seed(
        self,
        seed_name: str,
        beaker_name: str,
        seed_func: Callable[[], Iterable[BaseModel]],
    ) -> None:
        self.seeds[seed_name] = (beaker_name, seed_func)

    def list_seeds(self) -> dict[str, list[str]]:
        by_beaker = defaultdict(list)
        for seed_name, (beaker_name, _) in self.seeds.items():
            seed = self._db_get_seed(seed_name)
            if not seed:
                seed = Seed(name=seed_name)
            by_beaker[beaker_name].append(seed)
        return dict(by_beaker)

    def _db_get_seed(self, seed_name: str) -> Seed | None:
        cursor = self.db.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute("SELECT * FROM _seeds WHERE name = ?", (seed_name,))
        if row := cursor.fetchone():
            return Seed(**row)
        else:
            return None

    def run_seed(self, seed_name: str) -> None:
        try:
            beaker_name, seed_func = self.seeds[seed_name]
        except KeyError:
            raise SeedError(f"Seed {seed_name} not found")
        beaker = self.beakers[beaker_name]

        if seed := self._db_get_seed(seed_name):
            raise SeedError(f"{seed_name} already run at {seed.imported_at}")

        num_items = 0
        for item in seed_func():
            beaker.add_item(item)
            num_items += 1

        cursor = self.db.cursor()
        cursor.execute(
            "INSERT INTO _seeds (name, beaker_name, num_items) VALUES (?, ?, ?)",
            (seed_name, beaker_name, num_items),
        )
        self.db.commit()
        return num_items

    # section: commands #######################################################

    def reset(self) -> list[str]:
        reset_list = []
        with self.db:
            cursor = self.db.cursor()
            res = cursor.execute("DELETE FROM _seeds")
            if res.rowcount:
                reset_list.append(f"{res.rowcount} seeds")
            for beaker in self.beakers.values():
                if bl := len(beaker):
                    beaker.reset()
                    reset_list.append(f"{beaker.name} ({bl})")
        return reset_list

    def graph_data(self) -> list[dict]:
        nodes = {}

        for node in networkx.topological_sort(self.graph):
            beaker = self.beakers[node]

            nodes[node] = {
                "name": node,
                "temp": isinstance(beaker, TempBeaker),
                "len": len(beaker),
                "edges": [],
            }

            rank = 0
            for from_b, to_b, edge in self.graph.in_edges(node, data=True):
                if nodes[from_b]["rank"] > rank:
                    rank = nodes[from_b]["rank"]
            nodes[node]["rank"] = rank + 1

            for from_b, to_b, edge in self.graph.out_edges(node, data=True):
                edge["to_beaker"] = to_b
                nodes[node]["edges"].append(edge)

        # all data collected for display
        return sorted(nodes.values(), key=lambda x: (x["rank"], x["name"]))

    # section: running ########################################################

    def run_linear(
        self, start_beaker: str | None = None, end_beaker: str | None = None
    ) -> RunReport:
        """
        Run the recipe linearly.

        In a linear run, beakers are processed one at a time, based on a
        topological sort of the graph.

        This means any beaker without dependencies will be processed first,
        followed by beakers that depend on those beakers, and so on.

        Args:
            start_beaker: the name of the beaker to start processing at
            end_beaker: the name of the beaker to stop processing at
        """
        report = RunReport(
            start_time=datetime.datetime.now(),
            end_time=datetime.datetime.now(),
            start_beaker=start_beaker,
            end_beaker=end_beaker,
            nodes={},
        )

        log.info("run_linear", recipe=self)
        loop = asyncio.get_event_loop()

        started = False if start_beaker else True

        # go through each node in forward order, pushing data
        for node in networkx.topological_sort(self.graph):
            # store count of dispatched items
            report.nodes[node] = node_report = defaultdict(int)
            # only process nodes between start and end
            if not started:
                if node == start_beaker:
                    started = True
                    log.info("partial run start", node=node)
                else:
                    log.info("partial run skip", node=node, waiting_for=start_beaker)
                    continue
            if end_beaker and node == end_beaker:
                log.info("partial run end", node=node)
                break

            # get outbound edges
            edges = self.graph.out_edges(node, data=True)
            for from_b, to_b, e in edges:
                from_beaker = self.beakers[from_b]
                to_beaker = self.beakers[to_b]
                edge = e["edge"]
                already_processed = from_beaker.id_set() & to_beaker.id_set()

                node_report["_already_processed"] += len(already_processed)

                log.info(
                    "edge",
                    from_b=from_b,
                    to_b=to_b,
                    edge=edge,
                    to_process=len(from_beaker) - len(already_processed),
                    already_processed=len(already_processed),
                )

                # convert coroutine to function
                if inspect.iscoroutinefunction(edge.func):

                    def e_func(x):
                        return loop.run_until_complete(edge.func(x))

                else:
                    e_func = edge.func

                for id, item in from_beaker.items():
                    if id in already_processed:
                        continue
                    try:
                        result = e_func(item)
                        match edge.edge_type:
                            case EdgeType.transform:
                                # transform: add result to to_beaker (if not None)
                                if result is not None:
                                    to_beaker.add_item(result, id)
                                    node_report[to_b] += 1
                            case EdgeType.conditional:
                                # conditional: add item to to_beaker if e_func returns truthy
                                if result:
                                    to_beaker.add_item(item, id)
                                    node_report[to_b] += 1
                    except Exception as e:
                        for (
                            error_types,
                            error_beaker_name,
                        ) in edge.error_map.items():
                            if isinstance(e, error_types):
                                error_beaker = self.beakers[error_beaker_name]
                                error_beaker.add_item(
                                    ErrorType(
                                        item=item,
                                        exception=str(e),
                                        exc_type=str(type(e)),
                                    ),
                                    id,
                                )
                                node_report[error_beaker_name] += 1
                                break
                        else:
                            # no error handler, re-raise
                            raise
        return report
