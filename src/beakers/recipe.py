import csv
import json
import typer
import inspect
import sqlite3
import hashlib
import asyncio
import networkx  # type: ignore
from collections import defaultdict, Counter
from typing import Iterable, Callable, Type
from pydantic import BaseModel, ConfigDict
from structlog import get_logger

from .beakers import Beaker, SqliteBeaker, TempBeaker

log = get_logger()


def get_sha512(filename: str) -> str:
    with open(filename, "rb") as file:
        return hashlib.sha512(file.read()).hexdigest()


class Transform(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    transform_func: Callable
    error_map: dict[tuple, str]


class ErrorType(BaseModel):
    item: BaseModel
    exception: str
    exc_type: str


def if_cond_true(data_cond_tup: tuple[dict, bool]) -> dict | None:
    return data_cond_tup[0] if data_cond_tup[1] else None


def if_cond_false(data_cond_tup: tuple[dict, bool]) -> dict | None:
    return data_cond_tup[0] if not data_cond_tup[1] else None


class Recipe:
    def __init__(self, name: str, db_name: str = "beakers.db"):
        self.name = name
        self.graph = networkx.DiGraph()
        self.beakers: dict[str, Beaker] = {}
        self.seeds: defaultdict[str, list[Iterable[BaseModel]]] = defaultdict(list)
        self.db = sqlite3.connect(db_name)
        cursor = self.db.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS _metadata (table_name TEXT PRIMARY KEY, data JSON)"
        )

    def __repr__(self) -> str:
        return f"Recipe({self.name})"

    def add_beaker(
        self,
        name: str,
        datatype: Type[BaseModel],
        beaker_type: Type[Beaker] = SqliteBeaker,
    ) -> Beaker:
        self.graph.add_node(name, datatype=datatype)
        if datatype is None:
            self.beakers[name] = TempBeaker(name, datatype, self)
        else:
            self.beakers[name] = SqliteBeaker(name, datatype, self)
        return self.beakers[name]

    def add_transform(
        self,
        from_beaker: str,
        to_beaker: str,
        transform_func: Callable,
        *,
        name: str | None = None,
        error_map: dict[tuple, str] | None = None,
    ) -> None:
        if name is None:
            name = transform_func.__name__
            if name == "<lambda>":
                name = "λ"
        transform = Transform(
            name=name,
            transform_func=transform_func,
            error_map=error_map or {},
        )
        self.graph.add_edge(
            from_beaker,
            to_beaker,
            transform=transform,
        )

    def add_conditional(
        self,
        from_beaker: str,
        condition_func: Callable,
        if_true: str,
        if_false: str = "",
    ) -> None:
        # first add a transform to evaluate the conditional
        if condition_func.__name__ == "<lambda>":
            cond_name = f"cond-{from_beaker}"
        else:
            cond_name = f"cond-{from_beaker}-{condition_func.__name__}"
        self.add_beaker(cond_name, None)
        self.add_transform(
            from_beaker,
            cond_name,
            lambda data: (data, condition_func(data)),
            name=cond_name,
        )

        # then add two filtered paths that remove the condition result
        self.add_beaker(if_true, None)
        self.add_transform(
            cond_name,
            if_true,
            if_cond_true,
        )
        if if_false:
            self.add_transform(
                cond_name,
                if_false,
                if_cond_false,
            )

    def add_seed(self, beaker_name: str, data: Iterable[BaseModel]) -> None:
        self.seeds[beaker_name].append(data)

    def process_seeds(self) -> None:
        log.info("process_seeds", recipe=self.name)
        for beaker_name, seeds in self.seeds.items():
            for seed in seeds:
                self.beakers[beaker_name].add_items(seed)

    def get_metadata(self, table_name: str) -> dict:
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT data FROM _metadata WHERE table_name = ?",
            (table_name,),
        )
        try:
            data = cursor.fetchone()["data"]
            log.debug("get_metadata", table_name=table_name, data=data)
            return json.loads(data)
        except TypeError:
            log.debug("get_metadata", table_name=table_name, data={})
            return {}

    def save_metadata(self, table_name: str, data: dict) -> None:
        data_json = json.dumps(data)
        log.info("save_metadata", table_name=table_name, data=data_json)
        # sqlite upsert
        cursor = self.db.cursor()
        cursor.execute(
            "INSERT INTO _metadata (table_name, data) VALUES (?, ?) ON CONFLICT(table_name) DO UPDATE SET data = ?",  # noqa
            (table_name, data_json, data_json),
        )
        self.db.commit()

    def csv_to_beaker(self, filename: str, beaker_name: str) -> None:
        beaker = self.beakers[beaker_name]
        lg = log.bind(beaker=beaker, filename=filename)
        # three cases: empty, match, mismatch
        # case 1: empty
        if len(beaker) == 0:
            with open(filename, "r") as file:
                reader = csv.DictReader(file)
                added = 0
                for row in reader:
                    beaker.add_item(beaker.model(**row))
                    added += 1
            lg.info("from_csv", case="empty", added=added)
            meta = self.get_metadata(beaker.name)
            meta["sha512"] = get_sha512(filename)
            self.save_metadata(beaker.name, meta)
        else:
            old_sha = self.get_metadata(beaker.name).get("sha512")
            new_sha = get_sha512(filename)
            if old_sha != new_sha:
                # case 3: mismatch
                lg.info("from_csv", case="mismatch", old_sha=old_sha, new_sha=new_sha)
                raise Exception("sha512 mismatch")
            else:
                # case 2: match
                lg.info("from_csv", case="match")

    def show(self) -> None:
        seed_count = Counter(self.seeds.keys())
        typer.secho("Seeds", fg=typer.colors.GREEN)
        for beaker, count in seed_count.items():
            typer.secho(f"  {beaker} ({count})", fg=typer.colors.GREEN)
        graph_data = self.graph_data()
        for node in graph_data:
            if node["temp"]:
                typer.secho(node["name"], fg=typer.colors.CYAN)
            else:
                typer.secho(
                    f"{node['name']} ({node['len']})",
                    fg=typer.colors.GREEN if node["len"] else typer.colors.YELLOW,
                )
            for edge in node["edges"]:
                print(f"  -({edge['transform'].name})-> {edge['to_beaker']}")
                for k, v in edge["transform"].error_map.items():
                    if isinstance(k, tuple):
                        typer.secho(
                            f"    {' '.join(c.__name__ for c in k)} -> {v}",
                            fg=typer.colors.RED,
                        )
                    else:
                        typer.secho(f"    {k.__name__} -> {v}", fg=typer.colors.RED)

    def graph_data(self) -> list[dict]:
        nodes = {}

        for node in networkx.topological_sort(self.graph):
            beaker = self.beakers[node]
            temp = isinstance(beaker, TempBeaker)

            nodes[node] = {
                "name": node,
                "temp": temp,
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

    def run_once(
        self, start_beaker: str | None = None, end_beaker: str | None = None
    ) -> None:
        log.info("run_once", recipe=self)
        loop = asyncio.get_event_loop()

        started = False if start_beaker else True

        # go through each node in forward order, pushing data
        for node in networkx.topological_sort(self.graph):
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

            for from_b, to_b, edge in edges:
                transform = edge["transform"]

                from_beaker = self.beakers[from_b]
                to_beaker = self.beakers[to_b]
                already_processed = from_beaker.id_set() & to_beaker.id_set()

                log.info(
                    "transform",
                    from_b=from_b,
                    to_b=to_b,
                    to_process=len(from_beaker) - len(already_processed),
                    already_processed=len(already_processed),
                    transform=edge["transform"].name,
                )

                # convert coroutine to function
                if inspect.iscoroutinefunction(transform.transform_func):

                    def t_func(x):
                        return loop.run_until_complete(transform.transform_func(x))

                else:
                    t_func = transform.transform_func

                for id, item in from_beaker.items():
                    if id in already_processed:
                        continue
                    try:
                        transformed = t_func(item)
                        if transformed:
                            to_beaker.add_item(transformed, id)
                    except Exception as e:
                        for (
                            error_types,
                            error_beaker_name,
                        ) in transform.error_map.items():
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
                                break
                        else:
                            # no error handler, re-raise
                            raise
