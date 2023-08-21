import abc
import inspect
from operator import attrgetter
from typing import AsyncGenerator, Callable, Generator
from pydantic import BaseModel
from structlog import get_logger
from databeakers.exceptions import NoEdgeResult, BadSplitResult
from databeakers._models import ErrorType
from ._utils import callable_name
from ._record import Record

log = get_logger()

DEST_STOP = "__stop"  # sentinel value for stopping the pipeline


class EdgeResult(BaseModel):
    dest: str
    data: BaseModel | None
    id_: str | None


class Edge(BaseModel):
    whole_record: bool = False

    # TODO: this isn't working, which feels like a bug in abc but
    # needs more investigation as to how this should work.
    # @abc.abstractmethod
    # async def _run(
    #     self, id_: str, data: BaseModel | Record
    # ) -> AsyncGenerator[EdgeResult, None]:
    #     """
    #     Overridden to implement the edge logic.
    #     """

    @abc.abstractmethod
    def out_beakers(self) -> set[str]:
        """
        Return set of beakers that this edge will output to.
        """


class Transform(Edge):
    func: Callable
    to_beaker: str
    error_map: dict[tuple, str]
    name: str | None = None
    allow_filter: bool = False

    def __init__(
        self,
        func: Callable,
        to_beaker: str,
        *,
        name: str | None = None,
        error_map: dict[tuple, str] | None = None,
        whole_record: bool = False,
        allow_filter: bool = False,
    ):
        super().__init__(
            func=func,
            whole_record=whole_record,
            to_beaker=to_beaker,
            error_map=error_map or {},
        )
        self.name = name or callable_name(func)
        self.func = func
        self.allow_filter = allow_filter

    async def _run(
        self, id_: str, data: BaseModel | Record
    ) -> AsyncGenerator[EdgeResult, None]:
        lg = log.bind(
            edge=self.name, edge_type="transform", id_=id_, to_beaker=self.to_beaker
        )
        try:
            result = self.func(data)
            if inspect.isawaitable(result):
                result = await result
        except Exception as e:
            lg = lg.bind(exception=repr(e))
            for (
                error_types,
                error_beaker_name,
            ) in self.error_map.items():
                if isinstance(e, error_types):
                    lg.info("edge error", error_beaker=error_beaker_name)
                    yield EdgeResult(
                        dest=error_beaker_name,
                        data=ErrorType(
                            item=data, exception=str(e), exc_type=str(type(e))
                        ),
                        id_=id_,
                    )
                    # done after one error
                    return
            else:
                # no error handler, re-raise
                lg.critical("edge exception", exception=str(e))
                raise

        if isinstance(result, (Generator, AsyncGenerator)):
            num_yielded = 0
            if isinstance(result, Generator):
                for item in result:
                    yield EdgeResult(dest=self.to_beaker, data=item, id_=None)  # new id
                    num_yielded += 1
            else:
                async for item in result:
                    yield EdgeResult(dest=self.to_beaker, data=item, id_=None)  # new id
                    num_yielded += 1
            lg.info("edge transform (generator)", num_yielded=num_yielded)
            if not num_yielded:
                if self.allow_filter:
                    yield EdgeResult(dest=DEST_STOP, data=None, id_=id_)
                else:
                    raise NoEdgeResult("edge generator yielded no items")
        elif result is not None:
            # standard case -> forward result
            lg.info("edge transform")
            yield EdgeResult(dest=self.to_beaker, data=result, id_=id_)
        elif self.allow_filter:
            # if nothing is returned, and filtering is allowed, remove from stream
            lg.info("edge transform (removed)", result=None)
            yield EdgeResult(dest=DEST_STOP, data=None, id_=id_)
        else:
            raise NoEdgeResult("transform returned None")

    def out_beakers(self) -> set[str]:
        return {self.to_beaker} | set(self.error_map.values())


class Splitter(Edge):
    func: Callable
    splitter_map: dict[str | int | bool, Transform]
    name: str | None = None

    def __init__(
        self,
        func: Callable,
        splitter_map: dict[str | int | bool, Transform],
        *,
        name: str | None = None,
        whole_record: bool = False,
    ):
        super().__init__(
            whole_record=whole_record,
            func=func,
            splitter_map=splitter_map,
        )
        self.name = name or callable_name(func)

    async def _run(
        self, id_: str, data: BaseModel | Record
    ) -> AsyncGenerator[EdgeResult, None]:
        lg = log.bind(edge=self.name, edge_type="splitter", id_=id_)
        try:
            result = self.func(data)
            lg = lg.bind(splitter_result=result)
        except Exception as e:
            lg.critical(
                "splitter exception",
                exception=str(e),
            )
            raise

        if result not in self.splitter_map:
            lg.critical("splitter bad result")
            raise BadSplitResult(
                f"splitter result {result} not in splitter map {self.splitter_map}"
            )
        lg.info("splitter dispatch")
        async for item in self.splitter_map[result]._run(id_, data):
            yield item

    def out_beakers(self) -> set[str]:
        out = set()
        for transform in self.splitter_map.values():
            out.update(transform.out_beakers())
        return out


def _record_itemgetter(beaker: str, field: str) -> Callable[[Record], BaseModel]:
    return lambda record: getattr(record[beaker], field)


class FieldSplitter(Splitter):
    """
    Specialized splitter that splits on a field of the input record.
    """

    def __init__(
        self,
        field: str,
        splitter_map: dict[str | int | bool, Transform],
        *,
        beaker_name: str | None = None,
        whole_record: bool = False,
        name: str | None = None,
    ):
        if whole_record and not beaker_name:
            raise ValueError("beaker_name must be specified if whole_record=True")
        elif not whole_record and beaker_name:
            raise ValueError("beaker_name must not be specified if whole_record=False")
        elif whole_record and beaker_name:
            func = _record_itemgetter(beaker_name, field)
            if name is None:
                name = f"FieldSplitter({field}, beaker={beaker_name})"
        else:
            func = attrgetter(field)
            if name is None:
                name = f"FieldSplitter({field})"
        super().__init__(
            func=func,
            splitter_map=splitter_map,
            name=name,
            whole_record=whole_record,
        )


class Conditional(Splitter):
    """
    Specialized splitter that splits on a boolean condition.
    """

    def __init__(
        self,
        condition: Callable[[BaseModel | Record], bool],
        if_true: Transform,
        if_false: Transform,
        *,
        name: str | None = None,
        whole_record: bool = False,
    ):
        super().__init__(
            func=condition,
            splitter_map={
                True: if_true,
                False: if_false,
            },
            name=name,
            whole_record=whole_record,
        )
