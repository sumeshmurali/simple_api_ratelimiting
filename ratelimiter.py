import time
from abc import ABC, abstractmethod

from redis import Redis


class AbstractRatelimiter(ABC):
    @abstractmethod
    def is_ratelimit_exceeded(self) -> bool:
        pass

    @abstractmethod
    def incr_request_count(self):
        pass


class AbstractWindow(ABC):
    @abstractmethod
    def get_current_request_count(self) -> int:
        pass

    @abstractmethod
    def incr_request_count(self):
        pass


class SimpleSlidingWindow(AbstractWindow):

    def __init__(self, precision: int = 1, window_size: int = 60):
        """
        Create a new simple in memory sliding window
        Args:
            precision: Smaller this value, there will be more entries.
                (Higher value will result in increased performance)
            window_size: Size of the window in seconds.
        """
        self.window = {}
        self.precision = precision
        self.window_size = window_size

    def get_current_request_count(self) -> int:
        """Get the total number of requests in current window.

        Returns:
             total number of requests in current window
        """
        window_start_ts = (time.time() - self.window_size) // self.precision
        self.cleanup()
        keys_to_sum = [*filter(
            lambda x: x >= window_start_ts,
            self.window.keys()
        )]

        return sum([self.window[key] for key in keys_to_sum])

    def incr_request_count(self):
        """Increase the request count by 1"""
        timestamp = int(time.time() / self.precision)
        self.window[timestamp] = self.window.get(timestamp, 0) + 1

    def cleanup(self):
        """Cleans up values outside current window"""

        window_start_ts = (time.time() - self.window_size) // self.precision
        keys_to_delete = [*filter(
            lambda x: x < window_start_ts,
            self.window.keys()
        )]
        for key in keys_to_delete:
            del self.window[key]


class RedisSlidingWindow(AbstractWindow):
    def __init__(
            self,
            redis_connection: Redis,
            precision: int = 1,
            window_size: int = 60
    ):
        """
        Create a Sliding Window with Redis for storage.

        Args:
            redis_connection: Redis connection object
            precision: Smaller this value, there will be more entries. (Higher
                value will result in increased performance)
            window_size: Size of the window in seconds.
        """
        self.window_size = window_size
        self.precision = precision
        self.redis_connection = redis_connection
        self.window_key = "redis_key_for_sliding_window"
        self.lock = redis_connection.lock("redis_key_for_sliding_window_lock")

    def incr_request_count(self):
        """Increase the request count by 1"""
        with self.lock:
            timestamp = int(time.time() / self.precision)
            self.redis_connection.hincrby(self.window_key, str(timestamp), 1)

    def get_current_request_count(self) -> int:
        """Get the total number of requests in current window.

        Returns:
             total number of requests in current window
        """
        with self.lock:
            self.cleanup()
            window_start_ts = (
                                  time.time() - self.window_size
                              ) // self.precision
            window = self.redis_connection.hgetall(self.window_key)
            keys_to_sum = [*filter(
                lambda x: int(x) >= window_start_ts,
                window.keys())]
            return sum([int(window[key]) for key in keys_to_sum])

    def cleanup(self):
        """Cleans up values outside current window"""
        window_start_ts = (
                            time.time() - self.window_size
                        ) // self.precision
        window = self.redis_connection.hgetall(self.window_key)
        keys_to_delete = [*filter(
            lambda x: int(x) < window_start_ts,
            window.keys()
        )]
        if not keys_to_delete:
            return
        self.redis_connection.hdel(self.window_key, *keys_to_delete)


class SimpleRatelimiter(AbstractRatelimiter):
    def __init__(self, window: AbstractWindow, capacity: int):
        self.window = window
        self.capacity = capacity

    def is_ratelimit_exceeded(self) -> bool:
        """
        Checks if the ratelimit exceeded in current window

        Returns:
            True if ratelimit exceeded, else False

        """
        return self.window.get_current_request_count() >= self.capacity

    def incr_request_count(self):
        """Increments the request count in current window by 1"""
        self.window.incr_request_count()
