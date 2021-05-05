class CircuitBreakerStorage(object):
    """
    Defines the underlying storage for a circuit breaker - the underlying
    implementation should be in a subclass that overrides the method this
    class defines.
    """

    def __init__(self, name):
        """
        Creates a new instance identified by `name`.
        """
        self._name = name

    @property
    def name(self):
        """
        Returns a human friendly name that identifies this state.
        """
        return self._name

    @property
    def state(self):
        """
        Override this method to retrieve the current circuit breaker state.
        """
        pass

    @state.setter
    def state(self, state):
        """
        Override this method to set the current circuit breaker state.
        """
        pass

    def increment_counter(self):
        """
        Override this method to increase the failure counter by one.
        """
        pass

    def increment_fail_counter(self):
        """
        Override this method to increase the failure counter by one.
        """
        pass

    def reset_fail_counter(self):
        """
        Override this method to set the failure counter to zero.
        """
        pass

    @property
    def counter(self):
        """
        Override this method to retrieve the current value of the failure counter.
        """
        pass

    @property
    def fail_counter(self):
        """
        Override this method to retrieve the current value of the failure counter.
        """
        pass

    @property
    def opened_at(self):
        """
        Override this method to retrieve the most recent value of when the
        circuit was opened.
        """
        pass

    @opened_at.setter
    def opened_at(self, datetime):
        """
        Override this method to set the most recent value of when the circuit
        was opened.
        """
        pass
