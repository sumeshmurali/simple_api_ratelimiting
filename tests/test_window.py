import time

import pytest
from redis import Redis

from ratelimiter import RedisSlidingWindow, SimpleSlidingWindow


@pytest.fixture
def redis_connection():
    redis = Redis('127.0.0.1', 6379, 0, decode_responses=True)
    yield redis
    redis.flushall()
    redis.close()


def test_simple_sliding_window():
    window = SimpleSlidingWindow(precision=60, window_size=60)
    window.incr_request_count()
    window.incr_request_count()
    window.incr_request_count()
    assert window.get_current_request_count() == 3


def test_simple_sliding_window_with_expiring_keys():
    window = SimpleSlidingWindow(precision=1, window_size=5)
    window.incr_request_count()
    window.incr_request_count()
    window.incr_request_count()
    # waiting till the window expires
    time.sleep(6)
    window.incr_request_count()
    window.incr_request_count()
    assert window.get_current_request_count() == 2


def test_redis_sliding_window(redis_connection):
    window = RedisSlidingWindow(redis_connection, precision=60, window_size=60)
    window.incr_request_count()
    window.incr_request_count()
    window.incr_request_count()
    assert window.get_current_request_count() == 3


def test_redis_sliding_window_with_expiring_keys(redis_connection):
    window = RedisSlidingWindow(redis_connection, precision=1, window_size=5)
    window.incr_request_count()
    window.incr_request_count()
    window.incr_request_count()
    # waiting till the window expires
    time.sleep(6)
    window.incr_request_count()
    window.incr_request_count()
    assert window.get_current_request_count() == 2
