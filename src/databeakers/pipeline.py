import inspect
import sqlite3
import asyncio
import datetime
import networkx  # type: ignore
from collections import defaultdict
from typing import Iterable, Callable, Type
from databeakers.models import Edge, EdgeType, ErrorType, RunMode, RunReport, Seed
from pydantic import BaseModel
from structlog import get_logger

from .beakers import Beaker, SqliteBeaker, TempBeaker
from .record import Record
from .exceptions import ItemNotFound, SeedError

# !!! Note:
# by convention, a variable ending with _b is a beaker name
# & a variable ending with _beaker is a beaker instance
# _beaker_name is sometimes used to be explicit, and is also a name

log = get_logger()


class Pipeline:
    def __init__(self, name: str, db_name: str = "beakers.db", *, num_workers: int = 1):
        self.name = name
        self.num_workers = num_workers
        self.graph = networkx.DiGraph()
        self.beakers: dict[str, Beaker] = {}
        self.seeds: dict[str, tuple[str, Callable[[], Iterable[BaseModel]]]] = {}
        self.db = sqlite3.connect(db_name)
        self.db.row_factory = sqlite3.Row  # type: ignore
        cursor = self.db.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS _seeds (
                name TEXT, 
                beaker_name TEXT,
                num_items INTEGER,
                imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        cursor.execute("PRAGMA journal_mode=WAL;")
        # result = cursor.fetchone()
        # if tuple(result)[0] != "wal":
        #     raise SystemError(f"Unable to set WAL mode {result[0]}")

    def __repr__(self) -> str:
        return f"Pipeline({self.name})"

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
        whole_record: bool = False,
    ) -> None:
        if name is None:
            if hasattr(func, "__name__"):
                name = "λ" if func.__name__ == "<lambda>" else func.__name__
            else:
                name = repr(func)
        edge = Edge(
            name=name,
            edge_type=edge_type,
            func=func,
            error_map=error_map or {},
            whole_record=whole_record,
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

    def list_seeds(self) -> dict[str, list[Seed]]:
        by_beaker = defaultdict(list)
        for seed_name, (beaker_name, _) in self.seeds.items():
            seed = self._db_get_seed(seed_name)
            if not seed:
                seed = Seed(name=seed_name)
            by_beaker[beaker_name].append(seed)
        return dict(by_beaker)

    def _db_get_seed(self, seed_name: str) -> Seed | None:
        cursor = self.db.cursor()
        cursor.row_factory = sqlite3.Row  # type: ignore
        cursor.execute("SELECT * FROM _seeds WHERE name = ?", (seed_name,))
        if row := cursor.fetchone():
            return Seed(**row)
        else:
            return None

    def run_seed(self, seed_name: str) -> int:
        try:
            beaker_name, seed_func = self.seeds[seed_name]
        except KeyError:
            raise SeedError(f"Seed {seed_name} not found")
        beaker = self.beakers[beaker_name]

        if seed := self._db_get_seed(seed_name):
            raise SeedError(f"{seed_name} already run at {seed.imported_at}")

        num_items = 0
        with self.db:
            for item in seed_func():
                beaker.add_item(item)
                num_items += 1
            self.db.execute(
                "INSERT INTO _seeds (name, beaker_name, num_items) VALUES (?, ?, ?)",
                (seed_name, beaker_name, num_items),
            )

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

    def run(
        self,
        run_mode: RunMode,
        start_beaker: str | None = None,
        end_beaker: str | None = None,
    ) -> RunReport:
        """
        Run the pipeline in waterfall mode.

        In a waterfall run, beakers are processed one at a time, based on a
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
            run_mode=run_mode,
            nodes={},
        )
        log.info("run", pipeline=self)

        # go through each node in forward order
        if run_mode == RunMode.waterfall:
            return self._run_waterfall(start_beaker, end_beaker, report)
        elif run_mode == RunMode.river:
            return self._run_river(start_beaker, end_beaker, report)
        else:
            raise ValueError(f"Unknown run mode {run_mode}")

    def _run_waterfall(
        self, start_beaker: str | None, end_beaker: str | None, report: RunReport
    ) -> RunReport:
        started = False if start_beaker else True
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

            # push data from this node to downstream nodes
            report.nodes[node] = self._run_node_waterfall(node)

        return report

    def _get_full_record(self, id: str) -> Record:
        """
        Get the full record for a given id.

        This isn't the most efficient, but for waterfall runs
        the alternative is to store all records in memory.
        """
        rec = Record(id=id)
        for beaker_name, beaker in self.beakers.items():
            try:
                rec[beaker_name] = beaker.get_item(id)
            except ItemNotFound:
                pass
        return rec

    def _all_upstream(self, to_beaker: Beaker, edge: Edge):
        all_upstream = to_beaker.id_set()
        for error_b in edge.error_map.values():
            all_upstream |= self.beakers[error_b].id_set()
        return all_upstream

    def _run_node_waterfall(self, node: str) -> dict[str, int]:
        """
        Run a single node in a waterfall run, returning a report of items dispatched.
        """
        loop = asyncio.new_event_loop()
        # store count of dispatched items
        node_report: dict[str, int] = defaultdict(int)

        # get outbound edges
        edges = self.graph.out_edges(node, data=True)
        for from_b, to_b, e in edges:
            from_beaker = self.beakers[from_b]
            to_beaker = self.beakers[to_b]
            edge = e["edge"]
            all_upstream = self._all_upstream(to_beaker, edge)
            already_processed = from_beaker.id_set() & all_upstream
            node_report["_already_processed"] += len(already_processed)

            log.info(
                "processing edge",
                from_b=from_beaker.name,
                to_b=to_beaker.name,
                edge=edge.name,
                to_process=len(from_beaker) - len(already_processed),
                already_processed=len(already_processed),
            )
            partial_result = loop.run_until_complete(
                self._run_edge_waterfall(
                    from_beaker, to_beaker, edge, already_processed
                )
            )
            for k, v in partial_result.items():
                node_report[k] += v

        return node_report

    async def _run_edge_waterfall(
        self,
        from_beaker: Beaker,
        to_beaker: Beaker,
        edge: Edge,
        already_processed: set[str],
    ) -> dict[str, int]:
        queue: asyncio.Queue[tuple[str, Record]] = asyncio.Queue()
        node_report: dict[str, int] = defaultdict(int)

        # enqueue all items
        for id, item in from_beaker.items():
            if id in already_processed:
                continue
            queue.put_nowait((id, item))

        log.info("edge queue populated", edge=edge.name, queue_len=queue.qsize())

        # worker function
        async def queue_worker(name, queue):
            while True:
                try:
                    id, item = await queue.get()
                except RuntimeError:
                    # queue closed
                    return
                log.info("task accepted", worker=name, id=id, item=item, edge=edge.name)

                try:
                    with self.db:
                        result_loc = await self._run_edge_func(
                            from_beaker.name, edge, to_beaker, id, item=item
                        )
                    node_report[result_loc] += 1
                except Exception:
                    # uncaught exception, log and re-raise
                    result_loc = "UNCAUGHT_EXCEPTION"
                    raise
                finally:
                    queue.task_done()
                    log.info(
                        "task done", worker=name, id=id, item=item, sent_to=result_loc
                    )

        workers = [
            asyncio.create_task(queue_worker(f"worker-{i}", queue))
            for i in range(self.num_workers)
        ]

        # wait until the queue is fully processed or a worker raises
        queue_complete = asyncio.create_task(queue.join())
        await asyncio.wait(
            [queue_complete, *workers], return_when=asyncio.FIRST_COMPLETED
        )

        # cancel any remaining workers and pull exception to raise
        to_raise = None
        for w in workers:
            if not w.done():
                w.cancel()
            else:
                to_raise = w.exception()
        if to_raise:
            raise to_raise
        return node_report

    def _run_river(self, start_b, end_b, report: RunReport) -> RunReport:
        loop = asyncio.new_event_loop()
        if not start_b:
            start_b = list(networkx.topological_sort(self.graph))[0]
        start_beaker = self.beakers[start_b]
        report.nodes = defaultdict(lambda: defaultdict(int))

        for id in start_beaker.id_set():
            record = self._get_full_record(id)
            log.info("river record", id=id, record=record)
            for from_b, to_b in loop.run_until_complete(
                self._run_one_item_river(record, start_b, end_b)
            ):
                report.nodes[from_b][to_b] += 1

        return report

    async def _run_edge_func(
        self,
        cur_b: str,
        edge: Edge,
        to_beaker: Beaker,
        id: str,
        *,
        item: BaseModel | None = None,
        record: Record | None = None,
    ):
        """
        Used in river and waterfall runs, logic around an edge function
        hardly varies between them.

        One key difference is that in waterfall runs, record
        is always None, so will be fetched using _get_full_record.

        Returns: result_beaker_name
        """
        try:
            if edge.whole_record:
                if record is None:
                    record = self._get_full_record(id)
                result = edge.func(record)
            else:
                if item is None and record:
                    item = record[cur_b]
                result = edge.func(item)
            if inspect.isawaitable(result):
                result = await result
            if record:
                record[to_beaker.name] = result
        except Exception as e:
            log.info(
                "exception",
                exception=repr(e),
                edge=edge,
                id=id,
                item=item,
                record=record,
            )
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
                    return error_beaker.name
            else:
                # no error handler, re-raise
                raise

        # propagate result to downstream beakers
        match edge.edge_type:
            case EdgeType.transform:
                # transform: add result to to_beaker (if not None)
                if result is not None:
                    to_beaker.add_item(result, id)
                    to_beaker_name = to_beaker.name
                else:
                    to_beaker_name = "_none"
            case EdgeType.conditional:
                # conditional: add item to to_beaker if e_func returns truthy
                if result:
                    to_beaker.add_item(item, id)
                    to_beaker_name = to_beaker.name
                else:
                    to_beaker_name = "_none"
            case _:
                raise ValueError(f"Unknown edge type {edge.edge_type}")
        return to_beaker_name

    async def _run_one_item_river(
        self, record: Record, cur_b: str, end_b: str
    ) -> list[tuple[str, str]]:
        """
        Run a single item through a single beaker.

        Calls itself recursively to fan out to downstream beakers.

        Return list of (from, to) pairs.
        """
        subtasks = []
        stop_early = False
        from_to = []

        # fan an item out to all downstream beakers
        for _, to_b, e in self.graph.out_edges(cur_b, data=True):
            edge = e["edge"]
            to_beaker = self.beakers[to_b]

            # TODO: cache this upstream set?
            if record.id in self._all_upstream(to_beaker, edge):
                from_to.append((cur_b, "_already_processed"))
                # already processed this item, nothing to do
                continue

            if to_b == end_b:
                stop_early = True

            result_beaker = await self._run_edge_func(
                cur_b, edge, to_beaker, record.id, record=record
            )
            from_to.append((cur_b, result_beaker))
            subtasks.append(self._run_one_item_river(record, result_beaker, end_b))

        log.info(
            "river subtasks", cur_b=cur_b, subtasks=len(subtasks), stop_early=stop_early
        )
        if subtasks and not stop_early:
            results = await asyncio.gather(*subtasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    raise r
                else:
                    from_to.extend(r)

        return from_to
