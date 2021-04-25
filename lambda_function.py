from random import randint
from time import sleep

import numpy as np

from circuit_breaker import CircuitBreaker

DICT_VAR = {'SUCCESS': 90, 'FAIL': 10}
keys, weights = zip(*DICT_VAR.items())
probs = np.array(weights, dtype=float) / float(sum(weights))


def lambda_handler(event, context):
    key = event['key']

    try:
        random_states = np.random.choice(keys, 100, p=probs)
        states = [str(val) for val in random_states]

        for s in states:
            sleep(randint(1, 500) / 1000)
            cb = CircuitBreaker(key)

            cb.try_acquire_permission()
            if s == 'SUCCESS':
                cb.success()
            else:
                cb.fail()

        print('Access Allowed')
    except RuntimeError as error:
        print(f'Access Denied: {error}')
