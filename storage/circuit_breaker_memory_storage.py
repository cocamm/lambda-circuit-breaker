from storage.circuit_breaker_storage import CircuitBreakerStorage


class CircuitMemoryStorage(CircuitBreakerStorage):
    """
    Implements a `CircuitBreakerStorage` in local memory.
    """

    def __init__(self, state):
        """
        Creates a new instance with the given `state`.
        """
        super(CircuitMemoryStorage, self).__init__('memory')
        self._counter = 0
        self._fail_counter = 0
        self._opened_at = None
        self._state = state

    @property
    def state(self):
        """
        Returns the current circuit breaker state.
        """
        return self._state

    @state.setter
    def state(self, state):
        """
        Set the current circuit breaker state to `state`.
        """
        self._state = state

    def increment_counter(self):
        """
        Increases the failure counter by one.
        """
        self._counter += 1

    def increment_fail_counter(self):
        """
        Increases the failure counter by one.
        """
        self._fail_counter += 1

    def reset_counter(self):
        """
        Sets the failure counter to zero.
        """
        self._counter = 0

    def reset_fail_counter(self):
        """
        Sets the failure counter to zero.
        """
        self._fail_counter = 0

    def reset_counters(self):
        """
        Sets the failure and total counter to zero.
        """
        self._fail_counter = 0
        self._counter = 0

    @property
    def counter(self):
        """
        Returns the current value of the failure counter.
        """
        return self._counter

    @property
    def fail_counter(self):
        """
        Returns the current value of the failure counter.
        """
        return self._fail_counter

    @property
    def opened_at(self):
        """
        Returns the most recent value of when the circuit was opened.
        """
        return self._opened_at

    @opened_at.setter
    def opened_at(self, datetime):
        """
        Sets the most recent value of when the circuit was opened to
        `datetime`.
        """
        self._opened_at = datetime