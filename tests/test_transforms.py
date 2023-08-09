import time
import pytest
from databeakers.transforms import RateLimit, Retry
from databeakers.http import HttpRequest


async def assert_time_diff_between(func, min_diff, max_diff):
    start = time.time()
    await func()
    end = time.time()
    diff = end - start
    assert min_diff <= diff <= max_diff


@pytest.mark.asyncio
async def test_rate_limit_sync_edge_func():
    def edge_func(item):
        return item

    rate_limit = RateLimit(edge_func, requests_per_second=10)

    # ensure that the first call is not delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0, 0.001)
    # ensure that the second call is delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0.1, 0.2)


@pytest.mark.asyncio
async def test_rate_limit_async_edge_func():
    async def edge_func(item):
        return item

    rate_limit = RateLimit(edge_func, requests_per_second=10)

    # ensure that the first call is not delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0, 0.001)
    # ensure that the second call is delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0.1, 0.2)


def test_rate_limit_repr():
    rate_limit = RateLimit(lambda x: x, requests_per_second=10)
    assert repr(rate_limit) == "RateLimit(λ, 10)"


@pytest.mark.asyncio
async def test_retry_and_succeed():
    calls = 0

    async def fail_twice(item):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("fail")
        return item

    # need to retry 2 times to succeed
    retry = Retry(fail_twice, retries=2)
    assert await retry("x") == "x"
    assert calls == 3


@pytest.mark.asyncio
async def test_retry_and_still_fail():
    calls = 0

    async def fail_twice(item):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("fail")
        return item

    retry = Retry(fail_twice, retries=1)
    with pytest.raises(ValueError):
        await retry("x")
    assert calls == 2


def test_retry_repr():
    def edge_func(item):
        return item

    retry = Retry(edge_func, retries=1)
    assert repr(retry) == "Retry(edge_func, 1)"


def test_stacked_repr():
    assert repr(Retry(RateLimit(HttpRequest()), retries=1)) == (
        "Retry(RateLimit(HttpRequest(url), 1), 1)"
    )
