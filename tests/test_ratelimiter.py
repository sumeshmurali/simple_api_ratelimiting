from ratelimiter import SimpleRatelimiter, SimpleSlidingWindow


def test_ratelimiter_in_ratelimit():
    window = SimpleSlidingWindow(precision=1, window_size=5)
    ratelimiter = SimpleRatelimiter(window=window, capacity=5)
    ratelimiter.incr_request_count()
    ratelimiter.incr_request_count()
    assert not ratelimiter.is_ratelimit_exceeded()


def test_ratelimiter_out_ratelimit():
    window = SimpleSlidingWindow(precision=1, window_size=5)
    ratelimiter = SimpleRatelimiter(window=window, capacity=2)
    ratelimiter.incr_request_count()
    ratelimiter.incr_request_count()
    assert ratelimiter.is_ratelimit_exceeded()
