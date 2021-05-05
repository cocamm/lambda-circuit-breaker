import sys
import types
from datetime import datetime, timedelta

import gen as gen
import six

from cpybreaker.aggregator.circuit_breaker_aggregator import \
    CircuitBreakerCounterAggregator, CircuitBreakerTimerAggregator, WindowType
from cpybreaker.aggregator.circuit_breaker_aggregator import \
    ResultType

STATE_OPEN = 'open'
STATE_CLOSED = 'closed'
STATE_HALF_OPEN = 'half-open'


class CircuitBreakerState(object):
    """
    Implements the behavior needed by all circuit breaker states.
    """

    def __init__(self, cb, name):
        """
        Creates a new instance associated with the circuit breaker `cb` and
        identified by `name`.
        """
        self._breaker = cb
        self._name = name

    @property
    def name(self):
        """
        Returns a human friendly name that identifies this state.
        """
        return self._name

    def _handle_error(self, exc, reraise=True):
        """
        Handles a failed call to the guarded operation.
        """
        if self._breaker.is_system_error(exc):
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exc)
            self.on_failure(exc)
        else:
            self._handle_success()

        if reraise:
            raise exc

    def _handle_success(self):
        """
        Handles a successful call to the guarded operation.
        """
        self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    def call(self, func, *args, **kwargs):
        """
        Calls `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        """
        ret = None

        self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = func(*args, **kwargs)
            if isinstance(ret, types.GeneratorType):
                return self.generator_call(ret)

        except BaseException as e:
            self._handle_error(e)
        else:
            self._handle_success()
        return ret

    def call_async(self, func, *args, **kwargs):
        """
        Calls async `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.

        Return a closure to prevent import errors when using without tornado present
        """

        @gen.coroutine
        def wrapped():
            ret = None

            self.before_call(func, *args, **kwargs)
            for listener in self._breaker.listeners:
                listener.before_call(self._breaker, func, *args, **kwargs)

            try:
                ret = yield func(*args, **kwargs)
                if isinstance(ret, types.GeneratorType):
                    raise gen.Return(self.generator_call(ret))

            except BaseException as e:
                self._handle_error(e)
            else:
                self._handle_success()
            raise gen.Return(ret)

        return wrapped()

    def generator_call(self, wrapped_generator):
        try:
            value = yield next(wrapped_generator)
            while True:
                value = yield wrapped_generator.send(value)
        except StopIteration:
            self._handle_success()
            return
        except BaseException as e:
            self._handle_error(e, reraise=False)
            wrapped_generator.throw(e)

    def before_call(self, func, *args, **kwargs):
        """
        Override this method to be notified before a call to the guarded
        operation is attempted.
        """
        pass

    def on_success(self):
        """
        Override this method to be notified when a call to the guarded
        operation succeeds.
        """
        pass

    def on_failure(self, exc):
        """
        Override this method to be notified when a call to the guarded
        operation fails.
        """
        pass

    def _create_new_aggregator(self, window_type):
        """
        Return state object from state string, i.e.,
        'closed' -> <CircuitClosedState>
        """
        aggregator_map = {
            WindowType.COUNTER.value: CircuitBreakerCounterAggregator,
            WindowType.TIMER.value: CircuitBreakerTimerAggregator
        }
        try:
            cls = aggregator_map[window_type.value]
            return cls(self._breaker, self._breaker.window_size)
        except KeyError:
            msg = "Unknown window_type {!r}, valid window_types: {}"
            raise ValueError(msg.format(window_type, ', '.join(aggregator_map)))


class CircuitClosedState(CircuitBreakerState):
    """
    In the normal "closed" state, the circuit breaker executes operations as
    usual. If the call succeeds, nothing happens. If it fails, however, the
    circuit breaker makes a note of the failure.

    Once the number of failures exceeds a threshold, the circuit breaker trips
    and "opens" the circuit.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "closed" state.
        """
        super(CircuitClosedState, self).__init__(cb, STATE_CLOSED)
        self._aggregator = self._create_new_aggregator(
            self._breaker.window_type)

        if notify:
            # We only reset the counter if notify is True, otherwise the CircuitBreaker
            # will lose it's failure count due to a second CircuitBreaker being created
            # using the same _state_storage object, or if the _state_storage objects
            # share a central source of truth (as would be the case with the redis
            # storage).
            self._breaker._state_storage.reset_counters()

            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc):
        """
        Moves the circuit breaker to the "open" state once the failures
        threshold is reached.
        """
        self._aggregator.record(ResultType.ERROR)

        if self._aggregator.get_failure_threshold() >= self._breaker.fail_rate_threshold:
            self._breaker.open()

            error_msg = 'Failures threshold reached, circuit breaker opened'
            six.reraise(CircuitBreakerError, CircuitBreakerError(error_msg),
                        sys.exc_info()[2])

    def on_success(self):
        self._aggregator.record(ResultType.SUCCESS)

        # if self._breaker._state_storage.counter > self._breaker.minimum_number_of_calls:
        #     self._breaker._state_storage.reset_counters()


class CircuitOpenState(CircuitBreakerState):
    """
    When the circuit is "open", calls to the circuit breaker fail immediately,
    without any attempt to execute the real operation. This is indicated by the
    ``CircuitBreakerError`` exception.

    After a suitable amount of time, the circuit breaker decides that the
    operation has a chance of succeeding, so it goes into the "half-open" state.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "open" state.
        """
        super(CircuitOpenState, self).__init__(cb, STATE_OPEN)
        self._aggregator = self._create_new_aggregator(
            self._breaker.window_type)
        try:
            _ = self._breaker._state_storage.opened_at
        except:
            self._breaker._state_storage.opened_at = datetime.utcnow()
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def before_call(self, func, *args, **kwargs):
        """
        After the timeout elapses, move the circuit breaker to the "half-open"
        state; otherwise, raises ``CircuitBreakerError`` without any attempt
        to execute the real operation.
        """
        timeout = timedelta(seconds=self._breaker.reset_timeout)
        opened_at = self._breaker._state_storage.opened_at
        if opened_at and datetime.utcnow() < opened_at + timeout:
            error_msg = 'Timeout not elapsed yet, circuit breaker still open'
            raise CircuitBreakerError(error_msg)
        else:
            self._breaker.half_open()
            return self._breaker.call(func, *args, **kwargs)

    def call(self, func, *args, **kwargs):
        """
        Delegate the call to before_call, if the time out is not elapsed it will throw an exception, otherwise we get
        the results from the call performed after the state is switch to half-open
        """

        return self.before_call(func, *args, **kwargs)


class CircuitHalfOpenState(CircuitBreakerState):
    """
    In the "half-open" state, the next call to the circuit breaker is allowed
    to execute the dangerous operation. Should the call succeed, the circuit
    breaker resets and returns to the "closed" state. If this trial call fails,
    however, the circuit breaker returns to the "open" state until another
    timeout elapses.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "half-open" state.
        """
        super(CircuitHalfOpenState, self).__init__(cb, STATE_HALF_OPEN)

        self._breaker._state_storage.reset_counters()
        self._aggregator = CircuitBreakerCounterAggregator(self._breaker,
                                                           self._breaker.number_of_calls_half_open)
        if notify:
            for listener in self._breaker._listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc):
        """
        Opens the circuit breaker.
        """
        self._aggregator.record(ResultType.ERROR)

        self._check_failure_rate_threshold(
            self._aggregator.get_failure_threshold())

    def on_success(self):
        """
        Closes the circuit breaker.
        """
        self._aggregator.record(ResultType.SUCCESS)

        self._check_failure_rate_threshold(
            self._aggregator.get_failure_threshold())

    def _check_failure_rate_threshold(self, failure_threshold):
        if failure_threshold != -1:
            if failure_threshold < self._breaker.fail_rate_threshold:
                self._breaker.close()
            else:
                self._breaker.open()

                error_msg = 'Trial call failed, circuit breaker opened'
                six.reraise(CircuitBreakerError, CircuitBreakerError(error_msg),
                            sys.exc_info()[2])


class CircuitBreakerError(Exception):
    """
    When calls to a service fails because the circuit is open, this error is
    raised to allow the caller to handle this type of exception differently.
    """
    pass
