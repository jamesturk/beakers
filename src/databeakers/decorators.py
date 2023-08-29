import time
import asyncio
import inspect
import functools
from structlog import get_logger
from ._utils import callable_name

log = get_logger()


def rate_limit(edge_func, requests_per_second=1):
    last_call = None

    @functools.wraps(edge_func)
    async def new_func(item):
        nonlocal last_call
        if last_call is not None:
            diff = (1 / requests_per_second) - (time.time() - last_call)
            if diff > 0:
                log.debug("rate_limit sleep", seconds=diff, last_call=last_call)
                await asyncio.sleep(diff)
        last_call = time.time()
        result = edge_func(item)
        if inspect.isawaitable(result):
            return await result
        return result

    new_func.__name__ = f"rate_limit({callable_name(edge_func)}, {requests_per_second})"
    return new_func


def adaptive_rate_limit(
    edge_func,
    timeout_exceptions,
    *,
    requests_per_second=1,
    back_off_rate=2,
    speed_up_after=1,
):
    """
    - slow down by factor of back_off_rate on timeout
    - speed up by factor of back_off_rate on speed_up_after success
    """
    successes_counter = 0
    last_call = None
    desired_requests_per_second = requests_per_second

    async def new_func(item):
        nonlocal last_call
        nonlocal successes_counter
        nonlocal requests_per_second

        if last_call is not None:
            diff = (1 / requests_per_second) - (time.time() - last_call)
            if diff > 0:
                log.debug(
                    "adaptive_rate_limit sleep",
                    seconds=diff,
                    last_call=last_call,
                    streak=successes_counter,
                )
                await asyncio.sleep(diff)
        last_call = time.time()

        try:
            result = edge_func(item)
            if inspect.isawaitable(result):
                result = await result

            # check if we should speed up
            successes_counter += 1
            if (
                successes_counter >= speed_up_after
                and requests_per_second < desired_requests_per_second
            ):
                successes_counter = 0
                requests_per_second *= back_off_rate
                log.warning(
                    "adaptive_rate_limit speed up",
                    requests_per_second=requests_per_second,
                )

            return result
        except timeout_exceptions as e:
            requests_per_second /= back_off_rate
            log.warning(
                "adaptive_rate_limit slow down",
                exception=str(e),
                requests_per_second=requests_per_second,
            )
            raise e

    new_func.__name__ = (
        f"adaptive_rate_limit({callable_name(edge_func)}, {requests_per_second})"
    )
    return new_func


def retry(edge_func, retries):
    """
    Retry an edge a number of times.
    """

    @functools.wraps(edge_func)
    async def new_func(item):
        exception = None
        for n in range(retries + 1):
            try:
                return await edge_func(item)
            except Exception as e:
                exception = e
                log.error("retry", exception=str(e), retry=n + 1, max_retries=retries)
        # if we get here, we've exhausted our retries
        # (conditional appeases mypy)
        if exception:
            raise exception

    new_func.__name__ = f"retry({callable_name(edge_func)}, {retries})"
    return new_func
