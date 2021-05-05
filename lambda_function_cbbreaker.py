import os
from datetime import datetime
from random import randint
from time import sleep

import numpy as np
import redis

import c_breaker
from cb.circuit_breaker_percentage import PercentageCircuitBreaker
from cb.storage import RedisStorage

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

    breaker = PercentageCircuitBreaker(key,
                                       ttl=10,
                                       percent_failed=50,
                                       re_enable_after_seconds=7,
                                       storage=RedisStorage(),
                                       minimum_failures=5)
    for s in states:
        try:
            sleep(randint(1, 500) / 1000)

            print(f'{datetime.now()} CLOSED={breaker.is_closed}')

            if s != 'SUCCESS':
                breaker.register_error(0)
            else:
                breaker.register_success(0)
        except Exception as e:

            print(f'{datetime.now()} {e}')
            pass


