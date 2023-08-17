import inspect
from typing import AsyncGenerator, Callable, Generator
from enum import Enum
from pydantic import BaseModel
from structlog import get_logger
from databeakers.exceptions import NoEdgeResult
from databeakers.pipeline import ErrorType
from ._utils import callable_name
from ._record import Record

log = get_logger()


class Edge:
    def __init__(
        self,
        whole_record: bool = False,
    ):
        self.whole_record = whole_record


class SpecialForward(Enum):
    forward = "_forward"
    stop = "_stop"


class ForwardResult(BaseModel):
    beaker_name: str
    data: BaseModel
    id_: str | None


class Transform(Edge):
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
            whole_record=whole_record,
        )
        self.func = func
        self.name = name or callable_name(func)
        self.error_map = error_map or {}
        self.allow_filter = allow_filter

    async def _run(self, id_: str, data: BaseModel | Record) -> BaseModel:
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
                    yield ForwardResult(
                        error_beaker_name,
                        ErrorType(data=data, exception=str(e), exc_type=str(type(e))),
                        id_,
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
                    yield ForwardResult(SpecialForward.forward, item, None)  # new id
                    num_yielded += 1
            else:
                async for item in result:
                    yield ForwardResult(SpecialForward.forward, item, None)  # new id
                    num_yielded += 1
            log.info(
                "generator yielded",
                edge=self.name,
                id=id_,
                num_yielded=num_yielded,
            )
            if not num_yielded:
                if self.allow_filter:
                    yield ForwardResult(SpecialForward.stop, None, id_)
                else:
                    raise NoEdgeResult("edge generator yielded no items")
        elif result is not None:
            yield ForwardResult(SpecialForward.forward, result, id)  # new id
        elif self.allow_filter:
            yield ForwardResult(SpecialForward.stop, None, id)
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
