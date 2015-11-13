"""
Task tier.
"""

import asyncio
import logging

logger = logging.getLogger("cio.tier")


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
        if not asyncio.iscoroutinefunction(coro):
            raise ValueError("Function argument must be a coroutine")
        self.source_count = 0

    def __repr__(self):
        return '<TaskTier for %s, sources: %d, targets: %d>' % (
            self.coro.__name__, self.source_count, len(self.emit_targets))

    def __call__(self, *args, **kwargs):
        logger.info("CALL: %s" % self)
        logger.debug("args: %s, %s" % (args, kwargs))
        self.cell.incref()
        return self.schedule(*args, **kwargs)

    @asyncio.coroutine
    def schedule(self, *args, **kwargs):
        logger.debug('Wait on coordinator "enter": %s' % self)
        yield from self.cell.coord.enter(self)
        logger.debug('Acquired coordinator "enter": %s' % self)
        task = self.loop.create_task(self.coro(self, *args, **kwargs))
        task.add_done_callback(lambda t: self.loop.create_task(self.finish()))
        return task

    @asyncio.coroutine
    def finish(self):
        logger.debug('Wait on coordinator "exit": %s' % self)
        yield from self.cell.coord.exit(self)
        logger.debug('Acquired coordinator "exit": %s' % self)
        self.cell.decref()

    @asyncio.coroutine
    def emit(self, *args, **kwargs):
        """ This will normally execute any callbacks associated with this tier
        and then pause till the coordinator releases us.  If `defer` is true,
        the coordinator condition will be yielded to prior to running the
        callbacks. """
        logger.info("EMIT from: %s" % self)
        logger.debug("     args: %s, %s" % (args, kwargs))
        for tier in self.emit_targets:
            yield from tier(*args, **kwargs) # XXX May need to be task or both

    def source_from(self, source_tier):
        """ Schedule this tier to be called when another tier emits. """
        self.source_count += 1
        source_tier.add_emit_target(self)

    def add_emit_target(self, callback):
        """ Run a callback when this tier emits data. """
        logger.info("ADD EMIT TARGET: %s -> %s" % (self, callback))
        self.emit_targets.append(callback)
