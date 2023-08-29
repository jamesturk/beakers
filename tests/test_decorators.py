import time
import pytest
from databeakers.http import HttpRequest
from databeakers.decorators import rate_limit, retry, adaptive_rate_limit
from databeakers._utils import callable_name


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

    rl = rate_limit(edge_func, requests_per_second=10)

    # ensure that the first call is not delayed
    await assert_time_diff_between(lambda: rl("x"), 0, 0.001)
    # ensure that the second call is delayed
    await assert_time_diff_between(lambda: rl("x"), 0.1, 0.2)


@pytest.mark.asyncio
async def test_rate_limit_async_edge_func():
    async def edge_func(item):
        return item

    rl = rate_limit(edge_func, requests_per_second=10)

    # ensure that the first call is not delayed
    await assert_time_diff_between(lambda: rl("x"), 0, 0.001)
    # ensure that the second call is delayed
    await assert_time_diff_between(lambda: rl("x"), 0.1, 0.2)


def test_rate_limit_name():
    rl = rate_limit(lambda x: x, requests_per_second=10)
    assert callable_name(rl) == "rate_limit(λ, 10)"


def test_rate_limit_annotations():
    def edge_func(item: int) -> int:
        return item

    r = rate_limit(edge_func)
    assert edge_func.__annotations__ == r.__annotations__


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
    r = retry(fail_twice, retries=2)
    assert await r("x") == "x"
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

    r = retry(fail_twice, retries=1)
    with pytest.raises(ValueError):
        await r("x")
    assert calls == 2


def test_retry_repr():
    def edge_func(item):
        return item

    r = retry(edge_func, retries=1)
    assert callable_name(r) == "retry(edge_func, 1)"


def test_retry_annotation():
    def edge_func(item: int) -> int:
        return item

    r = retry(edge_func, retries=1)
    assert edge_func.__annotations__ == r.__annotations__


def test_stacked_repr():
    assert callable_name(retry(rate_limit(HttpRequest()), retries=1)) == (
        "retry(rate_limit(HttpRequest(url), 1), 1)"
    )


@pytest.mark.asyncio
async def test_rate_limit_and_retry():
    calls = 0

    async def fail_twice(item):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("fail")
        return item

    # this order matters, the rate limit should be applied first
    # so that retry can't happen until the rate limit is satisfied
    both = retry(rate_limit(fail_twice, requests_per_second=10), retries=2)
    # should have slept twice
    await assert_time_diff_between(lambda: both("x"), 0.2, 0.3)
    assert await both("x") == "x"
    # 2 retries and 2 successes
    assert calls == 4


@pytest.mark.asyncio
async def test_adaptive_rate_limit_slows_down():
    calls = 0

    async def fail_twice(item):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("fail")
        return item

    arl = retry(
        adaptive_rate_limit(
            fail_twice,
            timeout_exceptions=(ValueError,),
            requests_per_second=20,
            back_off_rate=2,
        ),
        retries=2,
    )
    # initial speed is 20/s = 0.05s sleep
    # so after two failures should have slept twice, 0.1s and 0.2s
    await assert_time_diff_between(lambda: arl("x"), 0.3, 0.4)
    assert await arl("x") == "x"
    # 2 retries and 2 successes
    assert calls == 4


@pytest.mark.asyncio
async def test_adaptive_rate_limit_speeds_up():
    calls = 0

    async def fail_twice(item):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("fail")
        return item

    arl = retry(
        adaptive_rate_limit(
            fail_twice,
            timeout_exceptions=(ValueError,),
            requests_per_second=20,
            back_off_rate=2,
            speed_up_after=2,
        ),
        retries=2,
    )
    # initial speed is 20/s = 0.05s sleep
    # so after two failures should have slept twice, 0.1s and 0.2s
    await assert_time_diff_between(lambda: arl("x"), 0.3, 0.4)
    # will sleep 0.2 again on next call
    await assert_time_diff_between(lambda: arl("x"), 0.2, 0.3)
    # with two successes, should speed up
    await assert_time_diff_between(lambda: arl("x"), 0.1, 0.2)
    await assert_time_diff_between(lambda: arl("x"), 0.1, 0.2)
    # and two more, back to intended speed
    await assert_time_diff_between(lambda: arl("x"), 0.05, 0.1)
