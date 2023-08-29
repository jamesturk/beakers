import inspect
import asyncio
import datetime
import itertools
import networkx  # type: ignore
import pydot
from collections import defaultdict
from typing import Any, Iterable, Callable, Type, Generator
from types import UnionType
from pydantic import BaseModel
from structlog import get_logger
from sqlite_utils import Database
from sqlite_utils.utils import chunks

from ._record import Record
from ._models import RunMode, RunReport, ErrorType, SeedRun, Seed
from ._utils import callable_name, pydantic_to_schema, pyd_wrap, required_parameters
from .beakers import Beaker, SqliteBeaker, TempBeaker
from .edges import Transform, Edge, Splitter, DEST_STOP
from .exceptions import ItemNotFound, SeedError, InvalidGraph


log = get_logger()


class Pipeline:
    def __init__(self, name: str, db_name: str = "beakers.db", *, num_workers: int = 1):
        self.name = name
        self.num_workers = num_workers
        self.graph = networkx.DiGraph()
        self.beakers: dict[str, Beaker] = {}
        self.seeds: dict[str, Seed] = {}
        self._db = Database(memory=True) if db_name == ":memory:" else Database(db_name)
        self._db.enable_wal()
        self._db.execute("PRAGMA synchronous=1;")
        self._db.conn.isolation_level = None
        self._seeds_t = self._db.table("_seed_runs").create(
            pydantic_to_schema(SeedRun),
            pk="run_repr",
            if_not_exists=True,
        )
        self._cached_upstream_ids: dict[int, set[str]] = defaultdict(set)

    def __repr__(self) -> str:
        return f"Pipeline({self.name})"

    # section: seeds ##########################################################

    def register_seed(
        self,
        callable: Callable[[], Iterable[BaseModel]],
        beaker_name: str,
        seed_name: str = "",
    ):
        """
        Register a seed function to be run.

        Args:
            callable: function that returns an iterable of pydantic models
            beaker_name: name of beaker to add items to
            seed_name: name of seed, defaults to callable name but can be overridden
        """
        name = seed_name or callable_name(callable)
        self.seeds[name] = Seed(name=name, func=callable, beaker_name=beaker_name)

    def list_seeds(self) -> dict[str, dict[str, list]]:
        """
        Create list of seeds and runs, suitable for output.
        """
        # without defaultdict so return type is clear
        by_beaker: dict[str, dict[str, list]] = {}
        for seed in self.seeds.values():
            if seed.beaker_name not in by_beaker:
                by_beaker[seed.beaker_name] = {}
            by_beaker[seed.beaker_name][seed.display_name] = self.get_seed_runs(
                seed_name=seed.name
            )
        return by_beaker

    def get_seed_runs(self, seed_name: str) -> list[SeedRun]:
        """
        Get all runs for a seed.
        """
        return list(
            pyd_wrap(self._seeds_t.rows_where("seed_name = ?", [seed_name]), SeedRun)
        )

    def get_seed_run(self, run_repr: str) -> SeedRun | None:
        """
        Get a single run by its representation.
        """
        try:
            return list(
                pyd_wrap(self._seeds_t.rows_where("run_repr = ?", [run_repr]), SeedRun)
            )[0]
        except IndexError:
            return None

    def run_seed(
        self,
        seed_name: str,
        *,
        max_items: int = 0,
        reset=False,
        chunk_size=100,
        save_bad_runs=True,
        parameters: dict[str, str] | None = None,
    ):
        """
        Run a seed function to populate a beaker.

        Args:
            seed_name: name of seed to run
            max_items: maximum number of items to process
            reset: whether to reset the beaker before running
            chunk_size: number of items to add to beaker per transaction
            save_bad_runs: whether to save runs that fail
            parameters: parameters to pass to seed function
        """
        try:
            seed = self.seeds[seed_name]
        except KeyError:
            raise SeedError(f"Seed {seed_name} not found")

        if parameters is None:
            parameters = {}
            run_repr = f"sr:{seed.name}"
        else:
            # parametrized runs will each have a unique ID per param
            # sort params so they are always in the same order
            param_order = ",".join(sorted(parameters.keys()))
            param_order = ",".join(f"{k}={v}" for k, v in sorted(parameters.items()))
            run_repr = f"sr:{seed.name}[{param_order}]"
        num_items = 0
        required = required_parameters(seed.func)
        if set(required) != set(parameters.keys()):
            raise SeedError(
                f"Seed {seed_name} requires parameters: {required} (got {list(parameters.keys())}))"
            )

        beaker = self.beakers[seed.beaker_name]
        if reset:
            # remove record of run, delete beaker items
            self._seeds_t.delete_where("run_repr = ?", [run_repr])
            self.delete_from_beaker(seed.beaker_name, parent=[run_repr])

        if already_run := self.get_seed_run(run_repr):
            raise SeedError(f"Seed {seed_name} already run: {already_run}")

        start_time = datetime.datetime.utcnow()
        error = ""

        try:
            for chunk in chunks(seed.func(**parameters), chunk_size):
                # transaction per chunk
                with self._db.conn:
                    self._db.execute("BEGIN TRANSACTION")
                    for item in chunk:
                        beaker.add_item(item, parent=run_repr, id_=None)
                        num_items += 1
                        if num_items == max_items:
                            break
                if num_items == max_items:
                    break
        except Exception as e:
            error = str(e) or repr(e)

            if not save_bad_runs:
                self.delete_from_beaker(seed.beaker_name, parent=[run_repr])
                num_items = 0
            else:
                # tail items won't be saved
                num_items -= num_items % chunk_size

        end_time = datetime.datetime.utcnow()
        sr = SeedRun(
            run_repr=run_repr,
            seed_name=seed.name,
            beaker_name=seed.beaker_name,
            num_items=num_items,
            start_time=start_time,
            end_time=end_time,
            error=str(error),
        )
        if save_bad_runs or not error:
            self._seeds_t.insert(dict(sr))
        return sr

    # section: graph ##########################################################

    def add_beaker(
        self,
        name: str,
        datatype: Type[BaseModel],
        beaker_type: Type[Beaker] = SqliteBeaker,
    ) -> Beaker:
        """
        Add a beaker to the graph.

        Args:
            name: name of beaker
            datatype: type of data stored in beaker
            beaker_type: type of beaker to use (default: SqliteBeaker)
        """
        self.graph.add_node(name, datatype=datatype, node_type="beaker")
        self.beakers[name] = beaker_type(name, datatype, self)
        return self.beakers[name]

    def add_transform(
        self,
        from_beaker: str,
        to_beaker: str,
        func: Callable,
        *,
        name: str | None = None,
        error_map: dict[tuple, str] | None = None,
        whole_record: bool = False,
        allow_filter: bool = True,
    ) -> None:
        """
        Add a transform to the graph.

        Args:
            from_beaker: name of beaker to transform from
            to_beaker: name of beaker to transform to
            func: function to transform data

        Keyword Args:
            name: name of transform (default: callable name)
            error_map: map of exceptions to beaker names (default: {})
            whole_record: whether to pass the whole record to the transform (default: False)
            allow_filter: whether to allow filtering of items (default: True)
        """
        edge = Transform(
            name=name,
            func=func,
            to_beaker=to_beaker,
            error_map=error_map or {},
            whole_record=whole_record,
            allow_filter=allow_filter,
        )
        self.add_out_transform(from_beaker, edge)

    def add_out_transform(self, from_beaker: str, edge: Edge) -> None:
        """
        Declaration Rules:
        - from_beaker must exist
        - to_beaker must exist or have a return annotation, in which case it will be created
        - edge.func must take a single parameter
        - edge.func parameter must be a subclass of from_beaker
        - edge.func must return to_beaker or None
        - edge.error_map must be a dict of (exception,) -> beaker_name
        - edge.error_map beakers will be created if they don't exist
        """

        # check from/parameter type
        if from_beaker not in self.beakers:
            raise InvalidGraph(f"{from_beaker} not found")
        from_model = self.beakers[from_beaker].model

        signature = inspect.signature(edge.func)
        param_annotations = [p.annotation for p in signature.parameters.values()]
        if len(param_annotations) != 1:
            raise InvalidGraph(
                f"Edge functions should only take (item) as parameters, {edge.func} "
                f"takes {len(param_annotations)}"
            )
        item_annotation = param_annotations[0]
        if item_annotation == inspect.Signature.empty:
            log.warning(
                "no parameter annotation on edge function",
                func=edge.func,
                name=edge.name,
            )
        elif item_annotation == Record or edge.whole_record:
            # if either is true, both must be
            if not edge.whole_record:
                raise InvalidGraph(
                    f"{edge.name} expects databeakers.record.Record, "
                    f"but edge.whole_record is False"
                )
            elif item_annotation != Record:
                raise InvalidGraph(
                    f"{edge.name} expects {item_annotation.__name__}, "
                    f"but edge.whole_record is True"
                )
        elif not issubclass(
            from_model, item_annotation
        ):  # accept subclasses as parameters
            raise InvalidGraph(
                f"{edge.name} expects {item_annotation.__name__}, "
                f"{from_beaker} contains {from_model.__name__}"
            )

        # check to/return type
        if edge.to_beaker not in self.beakers:
            if signature.return_annotation == inspect.Signature.empty:
                raise InvalidGraph(
                    f"{edge.to_beaker} not found & no return annotation on edge function to "
                    "infer type"
                )
            else:
                to_model = signature.return_annotation
                self.add_beaker(edge.to_beaker, to_model)
                log.debug(
                    "implicit beaker", beaker=edge.to_beaker, datatype=to_model.__name__
                )
        else:
            to_model = self.beakers[edge.to_beaker].model
            if signature.return_annotation == inspect.Signature.empty:
                log.warning(
                    "no return annotation on edge function",
                    func=edge.func,
                    name=edge.name,
                )
            else:
                ret_ann = signature.return_annotation
                # check if union type
                if type(ret_ann) == UnionType:
                    return_types = [rt for rt in ret_ann.__args__ if rt != type(None)]
                elif ret_ann.__name__ in ("Generator", "AsyncGenerator"):
                    return_types = [ret_ann.__args__[0]]
                else:
                    return_types = [ret_ann]
                for rt in return_types:
                    if not issubclass(to_model, rt):
                        raise InvalidGraph(
                            f"{edge.name} returns {rt.__name__}, "
                            f"{edge.to_beaker} expects {to_model.__name__}"
                        )

        # check error beakers
        for err_b in edge.error_map.values():
            if err_b not in self.beakers:
                log.debug("implicit error beaker", beaker=err_b)
                self.add_beaker(err_b, ErrorType)
            else:
                if self.beakers[err_b].model != ErrorType:
                    raise InvalidGraph(
                        f"Error beaker '{err_b}' must use beakers.pipeline.ErrorType"
                    )

        self.graph.add_edge(
            from_beaker,
            edge.to_beaker,
            edge=edge,
        )

    def add_splitter(self, from_beaker: str, splitter: Splitter) -> None:
        self.graph.add_node(splitter.name, node_type="split")
        self.graph.add_edge(from_beaker, splitter.name, edge=splitter)
        for out in splitter.splitter_map.values():
            self.graph.add_edge(
                splitter.name,
                out.to_beaker,
                edge=splitter,
            )

    # section: running ########################################################

    def run(
        self,
        run_mode: RunMode,
        only_beakers: list[str] | None = None,
    ) -> RunReport:
        """
        Run the pipeline.

        In a waterfall run, beakers are processed one at a time, based on a
        topological sort of the graph.

        This means any beaker without dependencies will be processed first,
        followed by beakers that depend on those beakers, and so on.

        Args:
            only_beakers: If provided, only run these beakers.
        """
        report = RunReport(
            start_time=datetime.datetime.now(),
            end_time=datetime.datetime.now(),
            only_beakers=only_beakers or [],
            run_mode=run_mode,
            nodes={},
        )

        # hack: clear graph's cache so run can be used multiple times
        self._cached_upstream_ids = {}

        # go through each node in forward order
        if run_mode == RunMode.waterfall:
            return self._run_waterfall(only_beakers, report)
        elif run_mode == RunMode.river:
            return self._run_river(only_beakers, report)
        else:
            raise ValueError(f"Unknown run mode {run_mode}")  # pragma: no cover

    def _run_waterfall(
        self, only_beakers: list[str] | None, report: RunReport
    ) -> RunReport:
        for node in self._beakers_toposort(only_beakers):
            # push data from this node to downstream nodes
            report.nodes[node] = self._run_node_waterfall(node)

        return report

    def _run_node_waterfall(self, node: str) -> dict[str, int]:
        """
        Run a single node in a waterfall run, returning a report of items dispatched.
        """
        loop = asyncio.new_event_loop()
        # store count of dispatched items
        node_report: dict[str, int] = defaultdict(int)

        # get outbound edges
        for edge in self._out_edges(node):
            from_beaker = self.beakers[node]
            all_upstream = self._all_upstream_ids(edge)
            already_processed = set(from_beaker.all_ids()) & all_upstream
            node_report["_already_processed"] = len(already_processed)

            log.info(
                "processing edge",
                from_b=from_beaker.name,
                edge=edge.name,
                to_process=len(from_beaker) - len(already_processed),
                already_processed=len(already_processed),
            )
            partial_result = loop.run_until_complete(
                self._run_edge_waterfall(from_beaker, edge, already_processed)
            )
            for k, v in partial_result.items():
                node_report[k] += v

        return node_report

    async def _run_edge_waterfall(
        self,
        from_beaker: Beaker,
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

        log.debug("edge queue populated", edge=edge.name, queue_len=queue.qsize())

        # worker function
        async def queue_worker(name, queue):
            while True:
                try:
                    id, item = await queue.get()
                except RuntimeError:
                    # queue closed
                    return  # pragma: no cover
                log.debug("task accepted", worker=name, id=id, edge=edge.name)

                try:
                    # transaction around each waterfall step
                    with self._db.conn:
                        result_loc = await self._run_edge_func(
                            from_beaker.name, edge, id, item=item
                        )
                    node_report[result_loc] += 1
                except Exception:
                    # uncaught exception, log and re-raise
                    result_loc = "UNCAUGHT_EXCEPTION"
                    raise
                finally:
                    queue.task_done()
                    log.debug("task done", worker=name, id=id, sent_to=result_loc)

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

    def _run_river(self, only_beakers, report: RunReport) -> RunReport:
        loop = asyncio.new_event_loop()

        # start beaker is the first beaker in the topological sort that's in only_beakers
        start_b = list(self._beakers_toposort(only_beakers))[0]
        log.debug("starting river run", start_beaker=start_b, only_beakers=only_beakers)

        start_beaker = self.beakers[start_b]
        report.nodes = defaultdict(lambda: defaultdict(int))

        all_ids = set(start_beaker.all_ids())
        already_processed = set()
        for edge in self._out_edges(start_b):
            already_processed |= self._all_upstream_ids(edge)
        unprocessed = all_ids - already_processed
        log.info(
            "starting river run",
            start_beaker=start_b,
            only_beakers=only_beakers,
            all_ids=len(all_ids),
            unprocessed=len(unprocessed),
        )
        for id in unprocessed:
            # transaction around river runs
            with self._db.conn:
                record = self._get_full_record(id)
                log.debug("river record", id=id)
                for from_b, to_b in loop.run_until_complete(
                    self._run_one_item_river(record, start_b, only_beakers)
                ):
                    report.nodes[from_b][to_b] += 1

        report.nodes[start_b]["_already_processed"] = len(already_processed)

        return report

    async def _run_edge_func(
        self,
        cur_b: str,
        edge: Edge,
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

        # figure out what is going to be passed in
        data: BaseModel | Record | None = None
        if edge.whole_record:
            data = record if record is not None else self._get_full_record(id)
            record = data
        else:
            data = item
            if data is None and record:
                data = record[cur_b]

        # run the edge function & push results to dest beakers
        async for e_result in edge._run(id, data):
            if e_result.dest == DEST_STOP:
                return DEST_STOP
            else:
                beaker = self.beakers[e_result.dest]
                beaker.add_item(e_result.data, parent=id, id_=e_result.id_)

        if record:
            record[e_result.dest] = e_result.data
        return e_result.dest

    async def _run_one_item_river(
        self, record: Record, cur_b: str, only_beakers: list[str] | None = None
    ) -> list[tuple[str, str]]:
        """
        Run a single item through a single beaker.

        Calls itself recursively to fan out to downstream beakers.

        Return list of (from, to) pairs.
        """
        subtasks = []
        from_to = []

        # fan an item out to all downstream beakers
        for edge in self._out_edges(cur_b):
            # TODO: cache this upstream set?
            if record.id in self._all_upstream_ids(edge):
                from_to.append((cur_b, "_already_processed"))
                # already processed this item, nothing to do
                continue

            result_beaker = await self._run_edge_func(
                cur_b, edge, record.id, record=record
            )
            from_to.append((cur_b, result_beaker))
            if only_beakers and result_beaker not in only_beakers:
                continue

            # add subtask to run next beaker in the chain
            if not only_beakers or result_beaker in only_beakers:
                subtasks.append(
                    self._run_one_item_river(record, result_beaker, only_beakers)
                )
            else:
                log.info(
                    "skipping beaker",
                    beaker=result_beaker,
                    only_beakers=only_beakers,
                )

        log.debug(
            "river subtasks",
            cur_b=cur_b,
            subtasks=len(subtasks),
        )
        if subtasks:
            results = await asyncio.gather(*subtasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    raise r
                else:
                    from_to.extend(r)

        return from_to

    # section: commands #######################################################

    def delete_from_beaker(
        self,
        beaker_name: str,
        *,
        parent: list[str] | None = None,
        ids: list[str] | None = None,
    ) -> None:
        """
        Delete items from a beaker, cascading to downstream beakers.

        Args:
            beaker_name: name of beaker to delete from
            parent: parent of items to delete
            ids: ids of items to delete
        """
        ids = self.beakers[beaker_name].delete(parent=parent, ids=ids)
        for edge in self._out_edges(beaker_name):
            self.delete_from_beaker(edge.to_beaker, parent=ids)

    def reset(self) -> list[str]:
        reset_list = []
        # transaction around entire reset
        with self._db.conn:
            self._seeds_t.delete_where()
            #    reset_list.append(f"seeds ({seed_count})")
            for beaker in self.beakers.values():
                if bl := len(beaker):
                    log.info("resetting", beaker=beaker.name, count=bl)
                    beaker.delete()
                    reset_list.append(f"{beaker.name} ({bl})")
        return reset_list

    def to_pydot(self, excludes: list[str] | None = None):
        if excludes is None:
            excludes = []
        pydg = pydot.Dot(graph_type="digraph", rankdir="LR")
        for b in self.beakers.values():
            if b.name in excludes:
                continue
            elif b.model == ErrorType:
                pydg.add_node(pydot.Node(b.name, color="red", group="errors"))
            elif isinstance(b, TempBeaker):
                pydg.add_node(pydot.Node(b.name, color="grey"))
            else:
                pydg.add_node(pydot.Node(b.name, color="blue"))

        for from_b, to_b, e in self.graph.edges(data=True):
            edge = e["edge"]
            if isinstance(edge, Transform):
                edge_name = f"{from_b} -> {to_b}"
                pydg.add_node(
                    pydot.Node(
                        edge_name, color="lightblue", shape="rect", label=edge.name
                    )
                )
                pydg.add_edge(pydot.Edge(from_b, edge_name))
                pydg.add_edge(pydot.Edge(edge_name, to_b))
                for _, error_beaker_name in edge.error_map.items():
                    if error_beaker_name not in excludes:
                        pydg.add_edge(
                            pydot.Edge(
                                edge_name,
                                error_beaker_name,
                                color="red",
                                samehead=error_beaker_name,
                            )
                        )
            elif isinstance(edge, Splitter):
                pydg.add_node(pydot.Node(edge.name, color="green", shape="diamond"))
                pydg.add_edge(pydot.Edge(from_b, to_b))
        return pydg

    def repair(self, dry_run: bool) -> dict[str, list[str]]:
        """
        Repair the database.

        Returns list of repairs.
        """
        seen_ids = set()
        orphaned = defaultdict(list)
        log.info("vacuuming db")
        self._db.vacuum()
        for beaker in self._beakers_toposort(None):
            log.info("scanning beaker", beaker=beaker, items=len(self.beakers[beaker]))
            for id_, parent in self.beakers[beaker].all_ids_and_parents():
                # every item must either have come from a seed run
                # or from another beaker that has already been processed
                if parent.startswith("sr:"):
                    # TODO: validate that seed run exists
                    seen_ids.add(id_)
                    continue
                elif parent in seen_ids:
                    seen_ids.add(id_)
                    # already processed
                    continue
                else:
                    orphaned[beaker].append(id_)

        for beaker, ids in orphaned.items():
            log.info(
                "removing orphaned items",
                beaker=beaker,
                count=len(ids),
                dry_run=dry_run,
            )
            if not dry_run:
                self.delete_from_beaker(beaker, ids=ids)

        return orphaned

    # section: helper methods ################################################

    def _beakers_toposort(
        self, only_beakers: list[str] | None
    ) -> Generator[str, None, None]:
        for node in networkx.topological_sort(self.graph):
            if self.graph.nodes[node]["node_type"] == "split":
                continue
            elif only_beakers and node not in only_beakers:
                continue
            else:
                yield node

    def _out_edges(self, cur_b):
        for _, _, e in self.graph.out_edges(cur_b, data=True):
            yield e["edge"]

    def _all_upstream_ids(self, edge: Edge):
        if id(edge) not in self._cached_upstream_ids:
            all_upstream = set()
            for error_b in edge.out_beakers():
                all_upstream |= self.beakers[error_b].parent_id_set()
            self._cached_upstream_ids[id(edge)] = all_upstream
        return self._cached_upstream_ids[id(edge)]

    def _get_full_record(self, id: str) -> Record:
        """
        Get the full record for a given id.

        This isn't the most efficient, but for waterfall runs
        the alternative is to store all records in memory.
        """
        rec = Record(id=id)
        exists = False
        for beaker_name, beaker in self.beakers.items():
            try:
                rec[beaker_name] = beaker.get_item(id)
                exists = True
            except ItemNotFound:
                pass
        if not exists:
            raise ItemNotFound(id)
        return rec

    def _grab_rows(
        self,
        beakers: list[str],
        *,
        offset: int,
        max_items: int,
        parameters: dict[str, Any] | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Grab rows from a list of beakers, joined together.
        """
        main_beaker, *aux_beakers = beakers

        # get initial ids
        beaker = self.beakers[main_beaker]
        ids: Iterable[str]
        if offset:
            # ensure order is consistent if paginating
            ids = beaker.all_ids(ordered=True, where=parameters)
        else:
            ids = beaker.all_ids(where=parameters)
        if max_items:
            ids = itertools.islice(ids, offset, offset + max_items)

        for id_ in ids:
            log.info(f"grabbing id {id_}")
            record = self._get_full_record(id_)
            as_dict = dict(id=id_, **dict(record[main_beaker]))  # type: ignore
            for aux_beaker in aux_beakers:
                if aux_beaker not in record:
                    continue
                for k, v in dict(record[aux_beaker]).items():  # type: ignore
                    as_dict[f"{aux_beaker}_{k}"] = v
            yield as_dict
