from flask import Flask
from redis import Redis

import config
from ratelimiter import RedisSlidingWindow, SimpleRatelimiter

app = Flask(__name__)
redis = Redis(
    config.redis_host,
    config.redis_port,
    config.redis_db,
    decode_responses=True
)

window = RedisSlidingWindow(
    redis_connection=redis,
    precision=config.precision,
    window_size=config.window_size
)

ratelimiter = SimpleRatelimiter(
    window=window,
    capacity=config.capacity,
)


@app.route('/')
def index():
    if ratelimiter.is_ratelimit_exceeded():
        return "Ratelimit exceeded", 429
    ratelimiter.incr_request_count()
    return "Welcome to the api"
