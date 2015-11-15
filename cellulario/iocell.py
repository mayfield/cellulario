"""
Some API handling code.  Predominantly this is to centralize common
alterations we make to API calls, such as filtering by router ids.
"""

import asyncio
import collections
from . import coordination, task_tier


class IOCell(object):
    """ A consolidated multi-level bundle of IO operations.  This is a useful
    facility when doing tiered IO calls such as http requests to get a list
    of things and then a fanout of http requests on each of those things and
    so forth.  The aim of this code is to provide simplified inputs and
    outputs to what is otherwise a complex arrangement of IO interactions
    and dependencies.   Namely the users of this code will use the generator
    output to they can iterate over the stream of finalized results as they
    are made available for export.

    Mechanically this produces a classic generator to the outside world that
    internally uses an async event loop to coordinate concurrent tasks.
    The tasks may be used to to cause further activity on the output stream.
    That is, the initial work orders may be tasks used to seed more work.

    Think of this as a portable io loop that's wrapped up and managed for the
    context of a single generator to the outside world.  The calling code
    will work in a normal blocking fashion (more or less). """

    TaskTier = task_tier.TaskTier

    def __init__(self, coord='noop', loop=None):
        self.loop = loop or asyncio.get_event_loop()
        if isinstance(coord, coordination.CellCoordinator):
            self.coord = coord
        else:
            self.coord = self.make_coord(coord)
        self.output_buffer = collections.deque()
        self.pending_exception = None
        self.tiers = []
        self.tiers_coro_map = {}
        self.refcnt = 0
        self.starters = []
        self.finalized = False

    def make_coord(self, name):
        return coordination.coordinators[name]()

    def incref(self):
        """ Used by TaskTier to indicate pending work. """
        self.refcnt += 1

    def decref(self):
        """ Used by TaskTier to indicate completed work. """
        self.refcnt -= 1
        if not self.refcnt or self.output_buffer:
            self.loop.stop()

    def done(self):
        return not self.refcnt

    def add_tier(self, coro, source=None, **spec):
        """ Add a coroutine to the cell as a task tier.  The source can be a
        single value or a list of either `TaskTier` types or coroutine
        functions already added to a `TaskTier` via `add_tier`. """
        assert not self.finalized
        tier = self.TaskTier(self, coro, spec)
        if source:
            if not hasattr(source, '__getitem__'):
                source = [source]
            source_tiers = []
            for x_source in source:
                if not isinstance(x_source, self.TaskTier):
                    x_source = self.tiers_coro_map[x_source]
                source_tiers.append(x_source)
            for x in source_tiers:
                tier.source_from(x)
        self.tiers.append(tier)
        self.tiers_coro_map[coro] = tier
        return tier

    def tier(self, *args, **kwargs):
        """ Function decorator for a tier cororoutine. """

        def decorator(coro):
            self.add_tier(coro, *args, **kwargs)
            return coro
        return decorator

    def tier_coroutine(self, *args, **kwargs):
        """ Combination of the `tier` decorator and asyncio.coroutine. """

        def decorator(fn):
            coro = asyncio.coroutine(fn)
            self.add_tier(coro, *args, **kwargs)
            return coro
        return decorator

    def finalize(self):
        """ Look at our tiers and setup the final data flow.  Once this is run
        a cell can not be modified again. """
        assert not self.finalized
        final_tiers = []
        for x in self.tiers:
            if not x.source_count:
                self.starters.append(x)
            if not x.target_count:
                final_tiers.append(x)
        self.add_tier(self.output_feed, source=final_tiers)
        self.coord.setup(self.tiers)
        self.finalized = True

    @asyncio.coroutine
    def output_feed(self, tier, *args, **kwargs):
        """ Simplify arguments and store them in the `output` buffer for
        yielding our user. """
        if not kwargs:
            arg = args[0] if len(args) == 1 else args
        else:
            arg = kwargs if not args else args, kwargs
        self.output_buffer.append(arg)

    def loop_exception_handler(self, loop, context):
        exc = context.get('exception')
        if exc:
            if not self.pending_exception:
                self.pending_exception = exc
                self.loop.stop()
        elif self.loop_exception_handler_save:
            return self.loop_exception_handler_save(loop, context)
        else:
            return self.loop.default_exception_handler(context)

    def run_loop(self):
        loop = self.loop
        save = self.loop_exception_handler_save = self.loop._exception_handler
        loop.set_exception_handler(self.loop_exception_handler)
        try:
            loop.run_forever()
        finally:
            self.loop_exception_handler_save = None
            loop.set_exception_handler(save)

    def output(self):
        """ Produce a classic generator for this cell's final results. """
        self.finalize()
        for x in self.starters:
            self.loop.create_task(x.enqueue_task())
        while True:
            if self.pending_exception:
                exc = self.pending_exception
                self.pending_exception = None
                try:
                    raise exc
                finally:
                    del exc
            while self.output_buffer:
                yield self.output_buffer.popleft()
            if not self.done():
                self.run_loop()
            elif not self.output_buffer:
                break

    def __iter__(self):
        return self.output()
