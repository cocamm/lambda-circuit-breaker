import logging
import os
import threading
from datetime import datetime
from enum import Enum

import redis

HASH_KEY = 'circuitbreaker'
STATE_KEY = 'state'
AGGREGATOR_KEY = 'window_aggregator'
DEFAULTS = {
    'failure_rate_threshold': 50,
    'sliding_window_size': 15,
    'minimum_number_of_calls': 20,
    'wait_duration_open_state': 20
}

redis_client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=os.getenv('REDIS_PORT', 6379),
                           decode_responses=True)


class CircuitBreaker:

    def __init__(self, key, options=None):
        if options is None:
            options = DEFAULTS
        self.key = key
        self.options = options
        self._lock = threading.Lock()

        current_state = self.__get_state()
        if current_state:
            self.state = State(int(current_state.get('state', State.CLOSED.value)))
            self.failure_count = int(current_state.get('failure_count', 0))
            self.success_count = int(current_state.get('success_count', 0))
            self.timestamp = datetime.now() if 'timestamp' not in current_state else datetime.strptime(
                current_state['timestamp'], "%Y-%m-%d'T'%H:%M:%S")
        else:
            self.__create()

        logging.debug(f'state={self.state} failure_count={self.failure_count} success_count={self.success_count}')

    def __create(self):
        self.close()

    def try_acquire_permission(self):
        if self.state == State.OPEN:
            raise RuntimeError('Circuit Breaker State OPEN')

    def close(self):
        self.success_count = 0
        self.failure_count = 0
        self.state = State.CLOSED
        self.timestamp = datetime.now()

    def open(self):
        self.state = State.OPEN
        self.timestamp = datetime.now()

    def success(self):
        self.__increment_success()
        if self.state != State.CLOSED:
            self.__set_error_aggregator(round(datetime.now().timestamp()), 'success_count')

    def fail(self):
        expire_at = None

        self.__increment_failure()
        self.__set_error_aggregator(round(datetime.now().timestamp()), 'failure_count')

        total_count = self.failure_count + self.success_count
        logging.debug(f'total_count={total_count}')

        if total_count > self.options['minimum_number_of_calls']:
            if self.__get_failure_threshold(total_count) >= self.options['failure_rate_threshold']:
                if self.state == State.CLOSED:
                    expire_at = self.options['wait_duration_open_state']
                    self.__remove_aggregators()

                logging.debug(f'Change status from {self.state} to OPEN')
                self.open()

            self.update_state(ex=expire_at)

    def update_state(self, ex=None):
        state = {
            'state': self.state.value,
            'timestamp': self.timestamp.strftime("%Y-%m-%d'T'%H:%M:%S")
        }
        self.__set_state(state)

        if ex is not None:
            self.__set_expire(ex)

    def __set_expire(self, ex):
        redis_client.expire(f'{HASH_KEY}:{self.key}:{STATE_KEY}', self.options['wait_duration_open_state'])

    def __get_state(self):
        state = redis_client.hgetall(f'{HASH_KEY}:{self.key}:{STATE_KEY}')

        keys = redis_client.keys(f'{HASH_KEY}:{self.key}:{AGGREGATOR_KEY}:*')
        logging.debug(f'keys={keys}')
        if keys is None:
            return

        pipe = redis_client.pipeline()
        for k in keys:
            pipe.hgetall(k)
        window_aggregators = pipe.execute()
        logging.debug(f'window_aggregators={window_aggregators}')

        if len(window_aggregators) > 0:
            state.update({'failure_count': 0, 'success_count': 0})
            for w in window_aggregators:
                state['failure_count'] += int(w.get('failure_count', 0))
                state['success_count'] += int(w.get('success_count', 0))

        logging.debug(f'state={state}')
        return state

    def __set_state(self, state):
        return redis_client.hmset(f'{HASH_KEY}:{self.key}:{STATE_KEY}', state)

    def __increment_success(self):
        with self._lock:
            self.success_count += 1

    def __increment_failure(self):
        with self._lock:
            self.failure_count += 1

    def __get_failure_threshold(self, total_count):
        failure_threshold = (self.failure_count * 100 / total_count)
        logging.debug(f'failure_threshold={failure_threshold}')

        return failure_threshold

    def __set_error_aggregator(self, aggregator_key, window_aggregator):
        result = redis_client.hincrby(f'{HASH_KEY}:{self.key}:{AGGREGATOR_KEY}:{aggregator_key}', window_aggregator, 1)
        if result == 1:
            redis_client.expire(f'{HASH_KEY}:{self.key}:{AGGREGATOR_KEY}:{aggregator_key}',
                                self.options['sliding_window_size'])
        return result

    def __remove_aggregators(self):
        keys = redis_client.keys(f'{HASH_KEY}:{self.key}:{AGGREGATOR_KEY}:*')
        if keys is None:
            return
        pipe = redis_client.pipeline()

        for k in keys:
            pipe.delete(k)
        pipe.execute()

        logging.info('Aggregators deleted...')


class State(Enum):
    CLOSED = 1
    OPEN = 2
    HALF_OPEN = 3
