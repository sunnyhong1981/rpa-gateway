"""Bounded idle polling; useful work immediately restores the active cadence."""


class IdleBackoff:
    def __init__(self, active, maximum):
        if not 0 < active <= maximum:
            raise ValueError('Polling bounds must be positive and ordered')
        self.active, self.maximum = active, maximum
        self.delay = active

    def next_delay(self, worked):
        self.delay = self.active if worked else min(self.maximum, self.delay * 2)
        return self.delay
