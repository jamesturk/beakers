import time
import asyncio
import inspect
from pydantic import BaseModel
from structlog import get_logger
from ._utils import callable_name

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
        return f"RateLimit({callable_name(self.edge_func)}, {self.requests_per_second})"

    async def __call__(self, item: BaseModel) -> BaseModel:
        if self.last_call is not None:
            diff = (1 / self.requests_per_second) - (time.time() - self.last_call)
            if diff > 0:
                log.debug("RateLimit sleep", seconds=diff, last_call=self.last_call)
                await asyncio.sleep(diff)
        self.last_call = time.time()
        result = self.edge_func(item)
        if inspect.isawaitable(result):
            return await result
        return result


class AdaptiveRateLimit:
    """ """

    def __init__(
        self,
        edge_func,
        timeout_exceptions,
        *,
        requests_per_second=1,
        back_off_rate=2,
        speed_up_after=1,
    ):
        self.edge_func = edge_func
        self.requests_per_second = requests_per_second
        self.desired_requests_per_second = requests_per_second
        self.timeout_exceptions = timeout_exceptions
        self.back_off_rate = back_off_rate
        self.speed_up_after = speed_up_after
        self.successes_counter = 0
        self.last_call = None
        """
        - slow down by factor of back_off_rate on timeout
        - speed up by factor of back_off_rate on speed_up_after success
        """

    def __repr__(self):
        return f"AdaptiveRateLimit({callable_name(self.edge_func)}, {self.requests_per_second})"

    async def __call__(self, item: BaseModel) -> BaseModel:
        if self.last_call is not None:
            diff = (1 / self.requests_per_second) - (time.time() - self.last_call)
            if diff > 0:
                log.debug(
                    "AdaptiveRateLimit sleep",
                    seconds=diff,
                    last_call=self.last_call,
                    streak=self.successes_counter,
                )
                await asyncio.sleep(diff)
        self.last_call = time.time()

        try:
            result = self.edge_func(item)
            if inspect.isawaitable(result):
                result = await result

            # check if we should speed up
            self.successes_counter += 1
            if (
                self.successes_counter >= self.speed_up_after
                and self.requests_per_second < self.desired_requests_per_second
            ):
                self.successes_counter = 0
                self.requests_per_second *= self.back_off_rate
                log.warning(
                    "AdaptiveRateLimit speed up",
                    requests_per_second=self.requests_per_second,
                )

            return result
        except self.timeout_exceptions as e:
            self.requests_per_second /= self.back_off_rate
            log.warning(
                "AdaptiveRateLimit slow down",
                exception=str(e),
                requests_per_second=self.requests_per_second,
            )
            raise e


class Retry:
    """
    Retry an edge a number of times.
    """

    def __init__(self, edge_func, retries=1):
        self.edge_func = edge_func
        self.retries = retries

    def __repr__(self):
        return f"Retry({callable_name(self.edge_func)}, {self.retries})"

    async def __call__(self, item: BaseModel) -> BaseModel:
        exception = None
        for n in range(self.retries + 1):
            try:
                return await self.edge_func(item)
            except Exception as e:
                exception = e
                log.error(
                    "Retry", exception=str(e), retry=n + 1, max_retries=self.retries
                )
        # if we get here, we've exhausted our retries
        # (conditional appeases mypy)
        if exception:
            raise exception
