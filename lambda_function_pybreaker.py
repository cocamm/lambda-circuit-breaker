import os
from datetime import datetime
from random import randint
from time import sleep

import numpy as np
import redis

import c_breaker

redis_client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=os.getenv('REDIS_PORT', 6379),
                           decode_responses=True)

DICT_VAR = {'SUCCESS': 15, 'FAIL': 85}
keys, weights = zip(*DICT_VAR.items())
probs = np.array(weights, dtype=float) / float(sum(weights))


def get_redis_connection(param):
    return redis_client


def lambda_handler(event, context):
    key = event['key']

    random_states = np.random.choice(keys, 100, p=probs)
    states = [str(val) for val in random_states]
    cb = c_breaker.CircuitBreaker(
        fail_max=10,
        reset_timeout=5,
        state_storage=c_breaker.CircuitRedisStorage(c_breaker.STATE_CLOSED, get_redis_connection('default'),
                                                namespace=key))
    for s in states:
        try:
            sleep(randint(1, 500) / 1000)

            cb.call(proccess_state, s)
        except Exception as e:

            print(f'{datetime.now()} {e}')
            pass


def proccess_state(state):
    if state != 'SUCCESS':
        raise RuntimeError('Error to proccess')
