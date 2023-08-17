import inspect
from typing import AsyncGenerator, Callable, Generator
from pydantic import BaseModel
from structlog import get_logger
from databeakers.exceptions import NoEdgeResult
from databeakers._models import ErrorType
from ._utils import callable_name
from ._record import Record

log = get_logger()


class Edge(BaseModel):
    whole_record: bool = False


class Destination:
    forward = "_forward"
    stop = "_stop"


class EdgeResult(BaseModel):
    dest: str
    data: BaseModel | None
    id_: str | None


class Transform(Edge):
    func: Callable
    error_map: dict[tuple, str]
    name: str | None = None
    allow_filter: bool = False

    def __init__(
        self,
        func: Callable,
        *,
        name: str | None = None,
        error_map: dict[tuple, str] | None = None,
        whole_record: bool = False,
        allow_filter: bool = False,
    ):
        super().__init__(
            func=func, whole_record=whole_record, error_map=error_map or {}
        )
        self.name = name or callable_name(func)
        self.func = func
        self.allow_filter = allow_filter

    async def _run(
        self, id_: str, data: BaseModel | Record
    ) -> AsyncGenerator[EdgeResult, None]:
        try:
            result = self.func(data)
        except Exception as e:
            lg = log.bind(
                exception=repr(e),
                id=id_,
                data=data,
            )
            for (
                error_types,
                error_beaker_name,
            ) in self.error_map.items():
                if isinstance(e, error_types):
                    lg.info("error handled", error_beaker=error_beaker_name)
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
                log.critical("unhandled error", exception=str(e))
                raise

        if inspect.isawaitable(result):
            result = await result

        if isinstance(result, (Generator, AsyncGenerator)):
            num_yielded = 0
            if isinstance(result, Generator):
                for item in result:
                    yield EdgeResult(
                        dest=Destination.forward, data=item, id_=None
                    )  # new id
                    num_yielded += 1
            else:
                async for item in result:
                    yield EdgeResult(
                        dest=Destination.forward, data=item, id_=None
                    )  # new id
                    num_yielded += 1
            log.info(
                "generator yielded",
                edge=self.name,
                id=id_,
                num_yielded=num_yielded,
            )
            if not num_yielded:
                if self.allow_filter:
                    yield EdgeResult(dest=Destination.stop, data=None, id_=id_)
                else:
                    raise NoEdgeResult("edge generator yielded no items")
        elif result is not None:
            # standard case -> forward result
            yield EdgeResult(dest=Destination.forward, data=result, id_=id_)
        elif self.allow_filter:
            # if nothing is returned, and filterin is allowed, remove from stream
            yield EdgeResult(dest=Destination.stop, data=None, id_=id_)
        else:
            raise NoEdgeResult("transform returned None")


class Splitter(Edge):
    def __init__(
        self,
        splitter_func: Callable,
        splitter_map: dict[str, Callable],
        *,
        name: str | None = None,
        whole_record: bool = False,
    ):
        super().__init__(whole_record=whole_record)
        self.splitter_func = splitter_func
        self.splitter_map = splitter_map
        self.name = name or callable_name(splitter_func)
