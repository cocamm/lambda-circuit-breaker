import threading
from datetime import datetime
from enum import Enum
from functools import wraps

from cpybreaker.aggregator.circuit_breaker_aggregator import WindowType
from cpybreaker.circuit_breaker_state import STATE_CLOSED, CircuitClosedState, \
    CircuitOpenState, CircuitHalfOpenState, STATE_HALF_OPEN, STATE_OPEN
from cpybreaker.storage.circuit_breaker_memory_storage import \
    CircuitMemoryStorage

try:
    from tornado import gen

    HAS_TORNADO_SUPPORT = True
except ImportError:
    HAS_TORNADO_SUPPORT = False

try:
    from redis.exceptions import RedisError

    HAS_REDIS_SUPPORT = True
except ImportError:
    HAS_REDIS_SUPPORT = False


class CircuitBreaker(object):

    def __init__(self, fail_rate_threshold=70, reset_timeout=60,
                 minimum_number_of_calls=100, window_size=60,
                 number_of_calls_half_open=10, window_type=WindowType.COUNTER,
                 exclude=None, listeners=None, state_storage=None, name=None):
        """
        Creates a new circuit breaker with the given parameters.
        """
        self._lock = threading.RLock()

        self._fail_rate_threshold = fail_rate_threshold
        self._reset_timeout = reset_timeout
        self._minimum_number_of_calls = minimum_number_of_calls
        self._number_of_calls_half_open = number_of_calls_half_open
        self._window_size = window_size
        self._window_type = window_type

        self._state_storage = state_storage or CircuitMemoryStorage(
            STATE_CLOSED)
        self._state = self._create_new_state(self.current_state)

        self._excluded_exceptions = list(exclude or [])
        self._listeners = list(listeners or [])
        self._name = name

    @property
    def counter(self):
        """
        Returns the current number of consecutive failures.
        """
        return self._state_storage.counter

    @property
    def fail_counter(self):
        """
        Returns the current number of consecutive failures.
        """
        return self._state_storage.fail_counter

    def get_counter_timer(self, time, window_size):
        """
        Returns the current number of consecutive failures.
        """
        return self._state_storage.get_counter_timer(time, window_size)

    def get_fail_counter_timer(self, time, window_size):
        """
        Returns the current number of consecutive failures.
        """
        return self._state_storage.get_fail_counter_timer(time, window_size)

    @property
    def fail_rate_threshold(self):
        """
        Returns the maximum number of failures tolerated before the circuit is
        opened.
        """
        return self._fail_rate_threshold

    @fail_rate_threshold.setter
    def fail_rate_threshold(self, number):
        """
        Sets the maximum `number` of failures tolerated before the circuit is
        opened.
        """
        self._fail_rate_threshold = number

    @property
    def reset_timeout(self):
        """
        Once this circuit breaker is opened, it should remain opened until the
        timeout period, in seconds, elapses.
        """
        return self._reset_timeout

    @reset_timeout.setter
    def reset_timeout(self, timeout):
        """
        Sets the `timeout` period, in seconds, this circuit breaker should be
        kept open.
        """
        self._reset_timeout = timeout

    @property
    def minimum_number_of_calls(self):
        """
        The minimum number of calls needed to circuit breaker take an action.
        """
        return self._minimum_number_of_calls

    @property
    def number_of_calls_half_open(self):
        """
        The number of permitted calls in half_open state.
        """
        return self._number_of_calls_half_open

    # @minimum_number_of_calls.setter
    # def minimum_number_of_calls(self, number):
    #     """
    #     Sets the minimum number of calls that the circuit breaker should use to
    #     process the state.
    #     """
    #     self._minimum_number_of_calls = number

    @property
    def window_size(self):
        """
        The window size to aggregate the failure and success count of calls
        """
        return self._window_size

    # @window_size.setter
    # def window_size(self, size):
    #     """
    #     Sets the window size to aggregate the results of calls.
    #     """
    #     self._window_size = size

    @property
    def window_type(self):
        """
        The window type to calculate the state of circuit breaker
        """
        return self._window_type

    def _create_new_state(self, new_state, prev_state=None, notify=False):
        """
        Return state object from state string, i.e.,
        'closed' -> <CircuitClosedState>
        """
        state_map = {
            STATE_CLOSED: CircuitClosedState,
            STATE_OPEN: CircuitOpenState,
            STATE_HALF_OPEN: CircuitHalfOpenState,
        }
        try:
            cls = state_map[new_state]
            return cls(self, prev_state=prev_state, notify=notify)
        except KeyError:
            msg = "Unknown state {!r}, valid states: {}"
            raise ValueError(msg.format(new_state, ', '.join(state_map)))

    @property
    def state(self):
        """
        Update (if needed) and returns the cached state object.
        """
        # Ensure cached state is up-to-date
        if self.current_state != self._state.name:
            # If cached state is out-of-date, that means that it was likely
            # changed elsewhere (e.g. another process instance). We still send
            # out a notification, informing others that this particular circuit
            # breaker instance noticed the changed circuit.
            self.state = self.current_state
        return self._state

    @state.setter
    def state(self, state_str):
        """
        Set cached state and notify listeners of newly cached state.
        """
        with self._lock:
            self._state = self._create_new_state(
                state_str, prev_state=self._state, notify=True)

    @property
    def current_state(self):
        """
        Returns a string that identifies the state of the circuit breaker as
        reported by the _state_storage. i.e., 'closed', 'open', 'half-open'.
        """
        return self._state_storage.state

    @property
    def excluded_exceptions(self):
        """
        Returns the list of excluded exceptions, e.g., exceptions that should
        not be considered system errors by this circuit breaker.
        """
        return tuple(self._excluded_exceptions)

    def add_excluded_exception(self, exception):
        """
        Adds an exception to the list of excluded exceptions.
        """
        with self._lock:
            self._excluded_exceptions.append(exception)

    def add_excluded_exceptions(self, *exceptions):
        """
        Adds exceptions to the list of excluded exceptions.
        """
        for exc in exceptions:
            self.add_excluded_exception(exc)

    def remove_excluded_exception(self, exception):
        """
        Removes an exception from the list of excluded exceptions.
        """
        with self._lock:
            self._excluded_exceptions.remove(exception)

    def _inc_counter(self):
        """
        Increments the counter of failed calls.
        """
        self._state_storage.increment_counter()

    def _inc_counter_timer(self, time):
        """
        Increments the counter of failed calls.
        """
        self._state_storage.increment_counter_timer(time)

    def _inc_fail_counter(self):
        """
        Increments the counter of failed calls.
        """
        self._state_storage.increment_fail_counter()

    def _inc_fail_counter_timer(self, time):
        """
        Increments the counter of failed calls.
        """
        self._state_storage.increment_fail_counter_timer(time)

    def is_system_error(self, exception):
        """
        Returns whether the exception `exception` is considered a signal of
        system malfunction. Business exceptions should not cause this circuit
        breaker to open.
        """
        exception_type = type(exception)
        for exclusion in self._excluded_exceptions:
            if type(exclusion) is type:
                if issubclass(exception_type, exclusion):
                    return False
            elif callable(exclusion):
                if exclusion(exception):
                    return False
        return True

    def call(self, func, *args, **kwargs):
        """
        Calls `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        """
        with self._lock:
            return self.state.call(func, *args, **kwargs)

    def call_async(self, func, *args, **kwargs):
        """
        Calls async `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.

        Return a closure to prevent import errors when using without tornado present
        """

        @gen.coroutine
        def wrapped():
            with self._lock:
                ret = yield self.state.call_async(func, *args, **kwargs)
                raise gen.Return(ret)

        return wrapped()

    def open(self):
        """
        Opens the circuit, e.g., the following calls will immediately fail
        until timeout elapses.
        """
        with self._lock:
            self.state = self._state_storage.state = STATE_OPEN
            self._state_storage.opened_at = datetime.utcnow()

    def half_open(self):
        """
        Half-opens the circuit, e.g. lets the following call pass through and
        opens the circuit if the call fails (or closes the circuit if the call
        succeeds).
        """
        with self._lock:
            self.state = self._state_storage.state = STATE_HALF_OPEN

    def close(self):
        """
        Closes the circuit, e.g. lets the following calls execute as usual.
        """
        with self._lock:
            self.state = self._state_storage.state = STATE_CLOSED

    def __call__(self, *call_args, **call_kwargs):
        """
        Returns a wrapper that calls the function `func` according to the rules
        implemented by the current state of this circuit breaker.

        Optionally takes the keyword argument `__pybreaker_call_coroutine`,
        which will will call `func` as a Tornado co-routine.
        """
        call_async = call_kwargs.pop('__pybreaker_call_async', False)

        if call_async and not HAS_TORNADO_SUPPORT:
            raise ImportError('No module named tornado')

        def _outer_wrapper(func):
            @wraps(func)
            def _inner_wrapper(*args, **kwargs):
                if call_async:
                    return self.call_async(func, *args, **kwargs)
                return self.call(func, *args, **kwargs)

            return _inner_wrapper

        if call_args:
            return _outer_wrapper(*call_args)
        return _outer_wrapper

    @property
    def listeners(self):
        """
        Returns the registered listeners as a tuple.
        """
        return tuple(self._listeners)

    def add_listener(self, listener):
        """
        Registers a listener for this circuit breaker.
        """
        with self._lock:
            self._listeners.append(listener)

    def add_listeners(self, *listeners):
        """
        Registers listeners for this circuit breaker.
        """
        for listener in listeners:
            self.add_listener(listener)

    def remove_listener(self, listener):
        """
        Unregisters a listener of this circuit breaker.
        """
        with self._lock:
            self._listeners.remove(listener)

    @property
    def name(self):
        """
        Returns the name of this circuit breaker. Useful for logging.
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        Set the name of this circuit breaker.
        """
        self._name = name


class ResultType(Enum):
    SUCCESS = "success",
    ERROR = "error"
