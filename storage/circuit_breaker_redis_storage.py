import logging
import time
import uuid
import calendar
from datetime import datetime

from circuit_breaker import HAS_REDIS_SUPPORT
from circuit_breaker_state import STATE_CLOSED
from storage.circuit_breaker_storage import CircuitBreakerStorage


class CircuitRedisStorage(CircuitBreakerStorage):
    """
    Implements a `CircuitBreakerStorage` using redis.
    """

    BASE_NAMESPACE = 'pybreaker'

    logger = logging.getLogger(__name__)

    def __init__(self, state, redis_object, namespace=None,
                 fallback_circuit_state=STATE_CLOSED):
        """
        Creates a new instance with the given `state` and `redis` object. The
        redis object should be similar to pyredis' StrictRedis class. If there
        are any connection issues with redis, the `fallback_circuit_state` is
        used to determine the state of the circuit.
        """

        # Module does not exist, so this feature is not available
        if not HAS_REDIS_SUPPORT:
            raise ImportError(
                "CircuitRedisStorage can only be used if the required dependencies exist")

        super(CircuitRedisStorage, self).__init__('redis')

        try:
            self.RedisError = __import__('redis').exceptions.RedisError
        except ImportError:
            # Module does not exist, so this feature is not available
            raise ImportError(
                "CircuitRedisStorage can only be used if 'redis' is available")

        self._redis = redis_object
        self._namespace_name = namespace
        self._fallback_circuit_state = fallback_circuit_state
        self._initial_state = str(state)

        self._initialize_redis_state(self._initial_state)

    def _initialize_redis_state(self, state):
        # self._redis.setnx(self._namespace('counter'), 0)
        # self._redis.setnx(self._namespace('fail_counter'), 0)
        self._redis.setnx(self._namespace('state'), state)

    @property
    def state(self):
        """
        Returns the current circuit breaker state.

        If the circuit breaker state on Redis is missing, re-initialize it
        with the fallback circuit state and reset the fail counter.
        """
        try:
            state_bytes = self._redis.get(self._namespace('state'))
        except self.RedisError:
            self.logger.error(
                'RedisError: falling back to default circuit state',
                exc_info=True)
            return self._fallback_circuit_state

        state = self._fallback_circuit_state
        if state_bytes is not None:
            state = state_bytes
        else:
            # state retrieved from redis was missing, so we re-initialize
            # the circuit breaker state on redis
            self._initialize_redis_state(self._fallback_circuit_state)

        return state

    @state.setter
    def state(self, state):
        """
        Set the current circuit breaker state to `state`.
        """
        try:
            self._redis.set(self._namespace('state'), str(state))
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def increment_counter(self):
        """
        Increases the failure counter by one.
        """
        try:
            self._redis.incr(self._namespace('counter'))
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def increment_fail_counter(self):
        """
        Increases the failure counter by one.
        """
        try:
            self._redis.incr(self._namespace('fail_counter'))
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def increment_counter_timer(self, time):
        """
        Increases the counter by one.
        """
        try:
            self._redis.zadd(self._namespace('counter-timer'),
                             {str(uuid.uuid4()): int(time)})
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def increment_fail_counter_timer(self, time):
        """
        Increases the failure counter by one.
        """
        try:
            self._redis.zadd(self._namespace('fail_counter-timer'),
                             {uuid.uuid4().bytes: int(time)})
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def reset_counter(self):
        """
        Sets the failure counter to zero.
        """
        try:
            self._redis.set(self._namespace('counter'), 0)
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def reset_fail_counter(self):
        """
        Sets the failure counter to zero.
        """
        try:
            self._redis.set(self._namespace('fail_counter'), 0)
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def reset_counters(self):
        """
        Sets the failure counter to zero.
        """
        try:
            pipe = self._redis.pipeline()
            pipe.set(self._namespace('counter'), 0)
            pipe.set(self._namespace('fail_counter'), 0)
            pipe.zremrangebyscore(self._namespace('counter-timer'), "-inf", "+inf")
            pipe.zremrangebyscore(self._namespace('fail_counter-timer'), "-inf", "+inf")
            pipe.execute()
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    @property
    def counter(self):
        """
        Returns the current value of the failure counter.
        """
        try:
            value = self._redis.get(self._namespace('counter'))
            if value:
                return int(value)
            else:
                return 0
        except self.RedisError:
            self.logger.error('RedisError: Assuming no errors', exc_info=True)
            return 0

    @property
    def fail_counter(self):
        """
        Returns the current value of the failure counter.
        """
        try:
            value = self._redis.get(self._namespace('fail_counter'))
            if value:
                return int(value)
            else:
                return 0
        except self.RedisError:
            self.logger.error('RedisError: Assuming no errors', exc_info=True)
            return 0

    def get_counter_timer(self, time, window_size):
        """
        Returns the current value of the failure counter.
        """
        try:
            key = self._namespace('counter-timer')
            p = self._redis.pipeline()
            p.zremrangebyscore(key, "-inf", int(time) - window_size)
            p.zcard(key)
            result = p.execute()
            value = result.pop()
            if value:
                self.logger.info(f'counter_value={value}')
                return int(value)
            else:
                return 0
        except self.RedisError:
            self.logger.error('RedisError: Assuming no errors', exc_info=True)
            return 0

    def get_fail_counter_timer(self, time, window_size):
        """
        Returns the current value of the failure counter.
        """
        try:
            key = self._namespace('fail_counter-timer')
            p = self._redis.pipeline()
            p.zremrangebyscore(key, "-inf", int(time) - window_size)
            p.zcard(key)
            result = p.execute()
            value = result.pop()
            if value:
                self.logger.info(f'fail_counter_value={value}')
                return int(value)
            else:
                return 0
        except self.RedisError:
            self.logger.error('RedisError: Assuming no errors', exc_info=True)
            return 0

    @property
    def opened_at(self):
        """
        Returns a datetime object of the most recent value of when the circuit
        was opened.
        """
        try:
            timestamp = self._redis.get(self._namespace('opened_at'))
            if timestamp:
                return datetime(*time.gmtime(int(timestamp))[:6])
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            return None

    @opened_at.setter
    def opened_at(self, now):
        """
        Atomically sets the most recent value of when the circuit was opened
        to `now`. Stored in redis as a simple integer of unix epoch time.
        To avoid timezone issues between different systems, the passed in
        datetime should be in UTC.
        """
        try:
            key = self._namespace('opened_at')

            def set_if_greater(pipe):
                current_value = pipe.get(key)
                next_value = int(calendar.timegm(now.timetuple()))
                pipe.multi()
                if not current_value or next_value > int(current_value):
                    pipe.set(key, next_value)

            self._redis.transaction(set_if_greater, key)
        except self.RedisError:
            self.logger.error('RedisError', exc_info=True)
            pass

    def _namespace(self, key):
        name_parts = [self.BASE_NAMESPACE, key]
        if self._namespace_name:
            name_parts.insert(0, self._namespace_name)

        return ':'.join(name_parts)
