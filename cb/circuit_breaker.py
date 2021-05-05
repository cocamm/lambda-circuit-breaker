from cb.storage import Storage


class CircuitBreaker(object):

    def __init__(self, service: str, breaker_type: str, storage: Storage = None, monitors=[], **kwargs):
        self.service = "{0}-{1}".format(service, breaker_type)
        self.storage = storage
        self.monitors = monitors
        self.storage.register_breaker(self.service, breaker_type, **kwargs)

    @property
    def is_closed(self):
        return True

    def register_success(self, elapsed: int):
        for monitor in self.monitors:
            monitor.success(self.service, elapsed)

    def register_error(self, elapsed: int):
        for monitor in self.monitors:
            monitor.failure(self.service, elapsed)

    def trip(self):
        for monitor in self.monitors:
            monitor.trip(self.service)

    def reset(self):
        for monitor in self.monitors:
            monitor.reset(self.service)
