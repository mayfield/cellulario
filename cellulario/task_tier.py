"""
Task tier.
"""

import asyncio
import functools
import logging

logger = logging.getLogger("cio.tier")


class TaskTier(object):
    """ A specific tier/type of cell operation.  Generally speaking a tier
    should be one or more IO operations that are equal in IO complexity. So
    while permissible to make multiple IO calls, they should not be
    interdependent or of different complexity. """

    def __init__(self, cell, func_or_coro):
        self.cell = cell
        self.emit_callbacks = []
        self.loop = cell.loop
        self.func_or_coro = func_or_coro
        if asyncio.iscoroutinefunction(func_or_coro):
            self._call = self.call_task
        else:
            self._call = self.call_func
        self.source_count = 0

    def __str__(self):
        return '<TaskTier for %s, sources: %d, targets: %d>' % (
            self.func_or_coro.__name__, self.source_count,
            len(self.emit_callbacks))

    def __call__(self, *args, **kwargs):
        logger.info("CALL: %s" % self)
        logger.debug("args: %s, %s" % (args, kwargs))
        self.cell.incref()
        return self._call(*args, **kwargs)

    @property
    def target_count(self):
        return len(self.emit_callbacks)

    def call_task(self, *args, **kwargs):
        task = self.loop.create_task(self.func_or_coro(self, *args, **kwargs))
        task.add_done_callback(lambda f: self.cell.decref())
        return task

    def call_func(self, *args, **kwargs):

        @functools.wraps(self.func_or_coro)
        def callback():
            try:
                self.func_or_coro(self, *args, **kwargs)
            finally:
                self.cell.decref()
        return self.loop.call_soon(callback)

    def emit(self, *args, **kwargs):
        logger.info("EMIT from: %s" % self)
        logger.debug("     args: %s, %s" % (args, kwargs))
        for x in self.emit_callbacks:
            x(*args, **kwargs)

    def source_from(self, source_tier):
        """ Schedule this tier to be called when another tier emits. """
        self.source_count += 1
        source_tier.add_emit_callback(self)

    def add_emit_callback(self, callback):
        """ Run a callback when this tier emits data. """
        logger.info("ADD EMIT CALLBACK: %s -> %s" % (self, callback))
        self.emit_callbacks.append(callback)
