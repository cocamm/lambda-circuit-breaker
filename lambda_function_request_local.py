from datetime import datetime
from random import randint
from time import sleep

import numpy as np
import requests

DICT_VAR = {'SUCCESS': 30, 'ERROR': 70}
DICT_VAR2 = {'SUCCESS': 70, 'ERROR': 30}
keys, weights = zip(*DICT_VAR.items())
probs = np.array(weights, dtype=float) / float(sum(weights))


def lambda_handler(event, context):
    key = event['key']

    random_states = np.random.choice(keys, 100, p=probs)
    states = [str(val) for val in random_states]

    for s in states:
        try:
            sleep(randint(1, 500) / 1000)

            r = requests.post("http://localhost:8080", json={'key': key, 'status': s})
        except Exception as e:

            print(f'{datetime.now()} {e}')
            pass

    # keys2, weights2 = zip(*DICT_VAR2.items())
    # probs2 = np.array(weights, dtype=float) / float(sum(weights))
    # random_states2 = np.random.choice(keys, 100, p=probs)
    # states2 = [str(val) for val in random_states]
    # for s in states:
    #     try:
    #         sleep(randint(1, 500) / 1000)
    #
    #         cb.call(proccess_state, s)
    #     except Exception as e:
    #
    #         print(f'{datetime.now()} {e}')
    #         pass
    #
    # print(f'estado={cb.current_state}')


if __name__ == '__main__':
    lambda_handler({'key': 'key'}, None)
