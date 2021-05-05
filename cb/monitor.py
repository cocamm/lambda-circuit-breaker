class Monitor:

    def success(self, service: str, elapsed: int):
        pass

    def failure(self, service: str, elapsed: int):
        pass

    def trip(self, service: str):
        pass

    def reset(self, service: str):
        pass
