import time
import asyncio
from pydantic import BaseModel
from structlog import get_logger

log = get_logger()


class RateLimit:
    """
    Limit the rate of flow based on the last call time.
    """

    def __init__(self, edge_func, requests_per_second=1):
        self.edge_func = edge_func
        self.requests_per_second = requests_per_second
        self.last_call = None

    def __repr__(self):
        return f"RateLimit({self.edge_func}, {self.requests_per_second})"

    async def __call__(self, item: BaseModel) -> BaseModel:
        if self.last_call is None:
            self.last_call = time.time()
        else:
            diff = self.requests_per_second - (time.time() - self.last_call)
            if diff > 0:
                log.debug("sleep", seconds=diff)
                await asyncio.sleep(diff)
        self.last_call = time.time()
        return await self.edge_func(item)


class Retry:
    """
    Retry an edge a number of times.
    """

    def __init__(self, edge_func, retries=1):
        self.edge_func = edge_func
        self.retries = retries

    def __repr__(self):
        return f"Retry({self.edge_func}, {self.retries})"

    async def __call__(self, item: BaseModel) -> BaseModel:
        exception = None
        for n in range(self.retries):
            try:
                return await self.edge_func(item)
            except Exception as e:
                exception = e
                log.error("retry", exception=e, retry=n + 1)
        if exception:
            raise exception
