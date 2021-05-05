import lambda_function_cbbreaker
import lambda_function_pybreaker


def test():
    event = {'key': 'key'}
    lambda_function_cbbreaker.lambda_handler(event, None)

if __name__ == '__main__':
    test()
