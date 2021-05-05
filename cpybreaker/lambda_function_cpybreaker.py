import os
from datetime import datetime
from random import randint
from time import sleep

import numpy as np
import redis

from cpybreaker import circuit_breaker
from cpybreaker.circuit_breaker import WindowType

redis_client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=os.getenv('REDIS_PORT', 6379),
                           decode_responses=True)

DICT_VAR = {'SUCCESS': 30, 'FAIL': 70}
DICT_VAR2 = {'SUCCESS': 90, 'FAIL': 10}
keys, weights = zip(*DICT_VAR.items())
probs = np.array(weights, dtype=float) / float(sum(weights))


def get_redis_connection(param):
    return redis_client


def lambda_handler(event, context):
    key = event['key']

    random_states = np.random.choice(keys, 100, p=probs)
    states = [str(val) for val in random_states]
    # cb = circuit_breaker.CircuitBreaker(
    #     fail_rate_threshold=50,
    #     reset_timeout=3,
    #     minimum_number_of_calls=10,
    #     window_type=WindowType.COUNTER,
    #     state_storage=circuit_breaker.CircuitMemoryStorage(circuit_breaker.STATE_CLOSED))
    cb = circuit_breaker.CircuitBreaker(
        fail_rate_threshold=50,
        reset_timeout=3,
        minimum_number_of_calls=10,
        window_type=WindowType.COUNTER,
        state_storage=circuit_breaker.CircuitRedisStorage(circuit_breaker.STATE_CLOSED, get_redis_connection('default'),
                                                          namespace=key))
    for s in states:
        try:
            sleep(randint(1, 500) / 1000)

            cb.call(proccess_state, s)
        except Exception as e:

            print(f'{datetime.now()} {e}')
            pass

    keys2, weights2 = zip(*DICT_VAR2.items())
    probs2 = np.array(weights2, dtype=float) / float(sum(weights2))
    random_states2 = np.random.choice(keys2, 100, p=probs2)
    states2 = [str(val) for val in random_states2]
    for s in states2:
        try:
            sleep(randint(1, 500) / 1000)

            cb.call(proccess_state, s)
        except Exception as e:

            print(f'{datetime.now()} {e}')
            pass

    print(f'estado={cb.current_state}')


def proccess_state(state):
    if state != 'SUCCESS':
        raise RuntimeError('Error to proccess')
    else:
        print("SUCESSO")


if __name__ == '__main__':
    lambda_handler({'key': 'key'}, None)
