import time
import pytest
from databeakers.transforms import RateLimit


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

    rate_limit = RateLimit(edge_func, requests_per_second=1)

    # ensure that the first call is not delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0, 0.001)
    # ensure that the second call is delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0.9, 1.05)


@pytest.mark.asyncio
async def test_rate_limit_async_edge_func():
    async def edge_func(item):
        return item

    rate_limit = RateLimit(edge_func, requests_per_second=1)

    # ensure that the first call is not delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0, 0.001)
    # ensure that the second call is delayed
    await assert_time_diff_between(lambda: rate_limit("x"), 0.9, 1.05)
