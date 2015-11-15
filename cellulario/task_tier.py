"""
Task tier.
"""

import asyncio


class TaskTier(object):
    """ A specific tier/type of cell operation.  Generally speaking a tier
    should be one or more IO operations that are equal in IO complexity. So
    while permissible to make multiple IO calls, they should not be
    interdependent or of different complexity.  The `spec` should be a dict
    to be used by a coordinator for adjusting the behavior of this tier. """

    @property
    def target_count(self):
        return len(self.emit_targets)

    def __init__(self, cell, coro, spec=None):
        self.cell = cell
        self.emit_targets = []
        self.loop = cell.loop
        self.coro = coro
        self.spec = spec
        self.backlog = 0
        if not asyncio.iscoroutinefunction(coro):
            raise ValueError("Function argument must be a coroutine")
        self.source_count = 0

    def __repr__(self):
        return '<TaskTier for %s, sources: %d, targets: %d>' % (
            self.coro.__name__, self.source_count, len(self.emit_targets))

    def enqueue_task(self, *args, **kwargs):
        """ Enqueue a task execution.  It will run in the background as soon
        as the coordinator clears it to do so. """
        self.cell.incref()
        return self._enqueue_task(*args, **kwargs)

    @asyncio.coroutine
    def _enqueue_task(self, *args, **kwargs):
        self.backlog += 1
        yield from self.cell.coord.enqueue(self)
        self.loop.create_task(self.coord_wrap(*args, **kwargs))

    @asyncio.coroutine
    def coord_wrap(self, *args, **kwargs):
        """ Wrap the coroutine with coordination throttles. """
        yield from self.cell.coord.enter(self)
        self.backlog -= 1
        yield from self.coro(self, *args, **kwargs)
        yield from self.cell.coord.exit(self)
        self.cell.decref()

    @asyncio.coroutine
    def emit(self, *args, **kwargs):
        """ Send data to the next tier(s).   This call be delayed if the
        coordinator thinks the backlog is too high for any of the emit
        targets. """
        for t in self.emit_targets:
            yield from t.enqueue_task(*args, **kwargs)

    def source_from(self, source_tier):
        """ Schedule this tier to be called when another tier emits. """
        self.source_count += 1
        source_tier.add_emit_target(self)

    def add_emit_target(self, tier):
        """ Run a callback when this tier emits data. """
        self.emit_targets.append(tier)
