import lambda_function


def test():
    event = { 'key': 'key'}
    lambda_function.lambda_handler(event, None)


if __name__ == '__main__':
    test()