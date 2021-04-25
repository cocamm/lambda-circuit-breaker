import threading
from datetime import datetime, timedelta
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
            self.next_attempt = datetime.strptime(current_state['next_attempt'], "%Y-%m-%d'T'%H:%M:%S")
        else:
            self.__create()

    def __create(self):
        self.state = State.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.next_attempt = datetime.now()

    def try_acquire_permission(self):
        if self.state == State.OPEN:
            if self.next_attempt <= datetime.now():
                self.half()
            else:
                raise RuntimeError('Circuit Breaker State OPEN')

    def close(self):
        self.success_count = 0
        self.failure_count = 0
        self.state = State.CLOSED

    def open(self):
        self.state = State.OPEN
        self.next_attempt = datetime.now() + timedelta(milliseconds=self.options['wait_duration_open_state'])

    def half(self):
        self.state = State.HALF_OPEN

    def success(self):
        action = 'SUCCESS'
        self.__increment_success()

        if self.state == State.HALF_OPEN:
            if self.success_count > self.options['success_rate_threshold_half_open']:
                self.close()
                action = 'RESET_SUCCESS'
        self.failure_count = 0
        self.update_state(action)

    def fail(self):
        self.__increment_failure()
        if self.failure_count >= self.options['failure_rate_threshold']:
            self.open()
        self.success_count = 0
        self.update_state('FAILURE')

    def update_state(self, action):
        if action == 'SUCCESS':
            state = {
                'state': self.state.value,
                'failure_count': self.failure_count,
                'next_attempt': self.next_attempt.strftime("%Y-%m-%d'T'%H:%M:%S")
            }
            self.__set_state(state)
        if action == 'RESET_SUCCESS':
            state = {
                'state': self.state.value,
                'failure_count': self.failure_count,
                'success_count': self.success_count,
                'next_attempt': self.next_attempt.strftime("%Y-%m-%d'T'%H:%M:%S")
            }
            self.__set_state(state)
        elif action == 'FAILURE':
            state = {
                'state': self.state.value,
                'success_count': self.success_count,
                'next_attempt': self.next_attempt.strftime("%Y-%m-%d'T'%H:%M:%S")
            }
            self.__set_state(state)
            if self.failure_count == 1:
                self.__set_expire(state)

    def __get_state(self):
        return redis_client.hgetall(f'{HASH_KEY}:{self.key}')

    def __set_state(self, state):
        return redis_client.hmset(f'{HASH_KEY}:{self.key}', state)

    def __set_expire(self, state):
        return redis_client.expire(f'{HASH_KEY}:{self.key}', 50)

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
