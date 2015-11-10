"""
Some API handling code.  Predominantly this is to centralize common
alterations we make to API calls, such as filtering by router ids.
"""

import asyncio
import collections
import logging
from . import coordination, task_tier

logger = logging.getLogger('cio.cell')


class IOCell(object):
    """ A consolidated multi-level bundle of IO operations.  This is a useful
    facility when doing tiered IO calls such as http requests to get a list
    of things and then a fanout of http requests on each of those things and
    so forth.  The aim of this code is to provide simplified inputs and
    outputs to what is otherwise a complex arrangement of IO interactions
    and dependencies.   Namely the users of this code will use the generator
    output to they can iterate over the stream of finalized results as they
    are made available.

    Mechanically this produces a classic generator to the outside world that
    internally uses an async event loop to coordinate concurrent tasks.
    The tasks may be used to to cause further activity on the output stream.
    That is, the initial work orders may be tasks used to seed more work.

    Think of this as a portable io loop that's wrapped up and managed for the
    context of a single generator to the outside world.  The calling code
    will work in a normal blocking fashion (more or less). """

    TaskTier = task_tier.TaskTier

    def __init__(self, coordinator='fifo', loop=None, debug=True):
        self.loop = loop or asyncio.get_event_loop()
        self.loop.set_debug(debug)
        self.coord = coordination.coordinators[coordinator]
        self.available = collections.deque()
        self.pending_exception = None
        self.tiers = []
        self.refcnt = 0
        self.starters = []
        self.finalized = False
        self.final_tier = self.TaskTier(self, self.make_available)

    def incref(self):
        """ Used by TaskTier to indicate pending work. """
        self.refcnt += 1
        logger.debug("INCREF: %d" % self.refcnt)

    def decref(self):
        """ Used by TaskTier to indicate completed work. """
        self.refcnt -= 1
        logger.debug("DECREF: %d" % self.refcnt)
        if not self.refcnt or self.available:
            self.loop.stop()

    def done(self):
        return not self.refcnt

    def add_tier(self, func, source=None):
        """ Add a function to the cell as a task tier. """
        assert not self.finalized
        tier = self.TaskTier(self, func)
        if source:
            if isinstance(source, self.TaskTier):
                source = [source]
            for x in source:
                tier.source_from(x)
        self.tiers.append(tier)
        return tier

    def finalize(self):
        """ Look at our tiers and setup the final data flow.  Once this is run
        a cell can not be modified again. """
        assert not self.finalized
        for x in self.tiers:
            if not x.source_count:
                self.starters.append(x)
            if not x.target_count:
                x.add_emit_callback(self.final_tier)
        self.finalized = True

    def make_available(self, tier, *args, **kwargs):
        """ Simplify arguments and store them in the `available` buffer for
        yielding our user. """
        if not kwargs:
            arg = args[0] if len(args) == 1 else args
        else:
            arg = kwargs if not args else args, kwargs
        logger.debug("AVAILABLE: %s" % arg)
        self.available.append(arg)

    def begin(self):
        for x in self.starters:
            x()

    def loop_exception_handler(self, loop, context):
        exc = context.get('exception')
        if exc:
            if self.pending_exception:
                logger.debug("IGNORING N+1 EXCEPTION: %s(%s)" % (
                             type(exc).__name__, exc))
            else:
                self.pending_exception = exc
                self.loop.stop()
        else:
            return self.loop_exception_handler_save(context)

    def run_loop(self):
        loop = self.loop
        save = self.loop_exception_handler_save = self.loop._exception_handler
        loop.set_exception_handler(self.loop_exception_handler)
        try:
            loop.run_forever()
        finally:
            self.loop_exception_handler_save = None
            loop.set_exception_handler(save)

    def __iter__(self):
        self.finalize()
        self.begin()
        while True:
            if self.pending_exception:
                exc = self.pending_exception
                self.pending_exception = None
                try:
                    raise exc
                finally:
                    del exc
            while self.available:
                yield self.available.popleft()
            if not self.done():
                self.run_loop()
            elif not self.available:
                break
