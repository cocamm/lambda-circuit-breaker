from circuit_breaker import CircuitBreaker


def lambda_handler(event, context):
    key = event['key']

    cb = CircuitBreaker(key)

    try:
        for i in range(40):
            cb.try_acquire_permission()
            cb.fail()

        print('Access Allowed')
    except RuntimeError as error:
        print(f'Access Denied: {error}')
