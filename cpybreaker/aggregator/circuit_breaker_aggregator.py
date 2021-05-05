import time
from enum import Enum


class ResultType(Enum):
    SUCCESS = "success",
    ERROR = "error"


class WindowType(Enum):
    """
    Window type to circuit breaker takes a decision.
    COUNTER: The state will be changed only when the total failures is greater
    than failure_rate_threshold
    TIMER: The state will be changed when in the window_size the percentage of
    total failures is greather than failure_rate_threshold
    """
    COUNTER = 1
    TIMER = 2


class CircuitBreakerCounterAggregator:

    def __init__(self, breaker, window_size):
        self.__breaker = breaker
        self.__total_count = 0
        self.__fail_count = 0
        self.__window_size = window_size
        self.__minimum_number_of_calls = min(self.__window_size,
                                             self.__breaker.minimum_number_of_calls)

    def record(self, result: ResultType):
        self.__breaker._inc_counter()

        if result == ResultType.ERROR:
            self.__breaker._inc_fail_counter()

        self.__total_count = self.__breaker.counter
        self.__fail_count = self.__breaker.fail_counter

        if self.__total_count >= self.__window_size:
            self.__breaker._state_storage.reset_counters()

    def get_failure_threshold(self):
        if self.__total_count == 0 or self.__total_count < self.__minimum_number_of_calls:
            return -1.0

        return (self.__fail_count * 100) / self.__total_count

    @property
    def total(self):
        return self.__total_count


class CircuitBreakerTimerAggregator:

    def __init__(self, breaker, window_size):
        self.__breaker = breaker
        self.__total_count = 0
        self.__fail_count = 0
        self.__window_size = window_size
        self.__minimum_number_of_calls = self.__breaker.minimum_number_of_calls
        self.__difference_in_seconds = 0

    def record(self, result: ResultType):
        timer = time.time()

        self.__breaker._inc_counter_timer(timer)

        if result == ResultType.ERROR:
            self.__breaker._inc_fail_counter_timer(timer)

        self.__total_count = self.__breaker.get_counter_timer(
            timer, self.__window_size)
        self.__fail_count = self.__breaker.get_fail_counter_timer(
            timer, self.__window_size)

    def get_failure_threshold(self):
        print(
            f'VALIDACAO TEMPO={self.__difference_in_seconds < self.__window_size and self.__total_count < self.__minimum_number_of_calls}')
        if self.__total_count == 0 or self.__total_count < self.__minimum_number_of_calls:
            return -1.0

        return (self.__fail_count * 100) / self.__total_count

    @property
    def total(self):
        return self.__total_count
