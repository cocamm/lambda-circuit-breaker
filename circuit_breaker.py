import threading
from datetime import datetime
from enum import Enum

import redis

HASH_KEY = 'circuitbreaker'

DEFAULTS = {
    'failure_rate_threshold': 50,
    'success_rate_threshold_half_open': 15,
    'wait_duration_open_state': 20000
}

redis_client = redis.Redis(host='localhost', port=6379, db=0)


class CircuitBreaker:

    def __init__(self, key, options=None):
        if options is None:
            options = DEFAULTS
        self.key = key
        self.options = options
        self._lock = threading.Lock()

        current_state = self.__get_state()
        if current_state:
            current_state = self.__convert(current_state)
            self.state = State(int(current_state['state']))
            self.failure_count = int(current_state['failure_count'])
            self.success_count = int(current_state['success_count'])
            self.timestamp = datetime.strptime(current_state['timestamp'], "%Y-%m-%d'T'%H:%M:%S")
        else:
            self.__create()

    def __create(self):
        self.state = State.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.timestamp = datetime.now()

    def try_acquire_permission(self):
        if self.state == State.OPEN:
            raise RuntimeError('Circuit Breaker State OPEN')

    def close(self):
        self.success_count = 0
        self.failure_count = 0
        self.state = State.CLOSED

    def open(self):
        self.state = State.OPEN
        self.timestamp = datetime.now()

    def success(self):
        action = 'SUCCESS'

        self.failure_count = 0
        self.__increment_success()
        self.update_state(action)

    def fail(self):
        if self.failure_count >= self.options['failure_rate_threshold']:
            if self.state == State.CLOSED:
                self.__set_expire()
            self.open()
            self.success_count = 0

        self.__increment_failure()
        self.update_state('FAILURE')

    def update_state(self, action):
        state = {}

        if action == 'SUCCESS':
            state = {
                'state': self.state.value,
                'failure_count': self.failure_count,
                'timestamp': self.timestamp.strftime("%Y-%m-%d'T'%H:%M:%S")
            }
        elif action == 'FAILURE':
            state = {
                'state': self.state.value,
                'success_count': self.success_count,
                'timestamp': self.timestamp.strftime("%Y-%m-%d'T'%H:%M:%S")
            }

        self.__set_state(state)

    def __get_state(self):
        return redis_client.hgetall(f'{HASH_KEY}:{self.key}')

    def __set_state(self, state):
        return redis_client.hmset(f'{HASH_KEY}:{self.key}', state)

    def __set_expire(self):
        return redis_client.expire(f'{HASH_KEY}:{self.key}', 20)

    def __increment_success(self):
        with self._lock:
            self.success_count += 1
        redis_client.hincrby(f'{HASH_KEY}:{self.key}', 'success_count', 1)

    def __increment_failure(self):
        with self._lock:
            self.failure_count += 1
        redis_client.hincrby(f'{HASH_KEY}:{self.key}', 'failure_count', 1)

    def __convert(self, data):
        if isinstance(data, bytes):
            return data.decode('ascii')
        if isinstance(data, dict):
            return dict(map(self.__convert, data.items()))
        if isinstance(data, tuple):
            return map(self.__convert, data)
        return data


class State(Enum):
    CLOSED = 1
    OPEN = 2
    HALF_OPEN = 3
