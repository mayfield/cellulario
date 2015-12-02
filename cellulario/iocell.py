"""
Some API handling code.  Predominantly this is to centralize common
alterations we make to API calls, such as filtering by router ids.
"""

import asyncio
import collections
import threading
import warnings
from . import coordination, tier


class IOCellEventLoopPolicy(type(asyncio.get_event_loop_policy())):
    """ This event loop policy is used during the context of an iocell
    operation to ensure calls to get_event_loop return the iocell's event
    loop. """

    def __init__(self, loop):
        self._iocell_loop = loop
        super().__init__()

    def __enter__(self):
        self.elp_save = asyncio.get_event_loop_policy()
        asyncio.set_event_loop_policy(self)

    def __exit__(self, *na):
        asyncio.set_event_loop_policy(self.elp_save)
        self.elp_save = None

    def set_event_loop(self):
        raise RuntimeError('Setting event loop in iocell context is invalid')

    def get_event_loop(self):
        return self._iocell_loop


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

    Think of this as a portable IO loop that's wrapped up and managed for the
    context of a single generator to the outside world.  The calling code
    will work in normal blocking style. """

    Tier = tier.Tier

    def __init__(self, coord='noop', debug=True):
        if isinstance(coord, coordination.AbstractCellCoordinator):
            self.coord = coord
        else:
            self.coord = self.make_coord(coord)
        self.debug = debug
        self.output_buffer = collections.deque()
        self.pending_exception = None
        self.closed = False
        self.tiers = []
        self.tiers_coro_map = {}
        self.cleaners = []
        self.finalized = False
        self.init_event_loop()

    def init_event_loop(self):
        """ Every cell should have its own ioloop for proper containment.
        The type of event loop is not so important however. """
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(self.debug)
        self.loop._set_coroutine_wrapper(self.debug)
        self.loop_policy = IOCellEventLoopPolicy(self.loop)
        self.loop_exception_handler_save = self.loop._exception_handler
        self.loop.set_exception_handler(self.loop_exception_handler)

    def cleanup_event_loop(self):
        """ Cleanup an event loop and close it down forever. """
        for task in asyncio.Task.all_tasks(loop=self.loop):
            if self.debug:
                warnings.warn('Cancelling task: %s' % task)
            task._log_destroy_pending = False
            task.cancel()
        self.loop.close()
        self.loop.set_exception_handler(self.loop_exception_handler_save)
        self.loop_exception_handler_save = None
        self.loop_policy = None
        self.loop = None

    def make_coord(self, name):
        return coordination.coordinators[name]()

    def done(self):
        return all(x.done() for x in asyncio.Task.all_tasks(loop=self.loop))

    def assertNotFinalized(self):
        """ Ensure the cell is not used more than once. """
        if self.finalized:
            raise RuntimeError('Already finalized: %s' % self)

    def add_tier(self, coro, **kwargs):
        """ Add a coroutine to the cell as a task tier.  The source can be a
        single value or a list of either `Tier` types or coroutine functions
        already added to a `Tier` via `add_tier`. """
        self.assertNotFinalized()
        assert asyncio.iscoroutinefunction(coro)
        tier = self.Tier(self, coro, **kwargs)
        self.tiers.append(tier)
        self.tiers_coro_map[coro] = tier
        return tier

    def append_tier(self, coro, **kwargs):
        """ Implicitly source from the tail tier like a pipe. """
        source = self.tiers[-1] if self.tiers else None
        return self.add_tier(coro, source=source, **kwargs)

    def add_cleaner(self, coro):
        """ Add a coroutine to run after the cell is done.  This is for the
        user to perform any cleanup such as closing sockets. """
        self.assertNotFinalized()
        self.cleaners.append(coro)

    def tier(self, *args, append=True, source=None, **kwargs):
        """ Function decorator for a tier coroutine.  If the function being
        decorated is not already a coroutine function it will be wrapped. """
        if len(args) == 1 and not kwargs and callable(args[0]):
            raise TypeError('Uncalled decorator syntax is invalid')

        def decorator(coro):
            if not asyncio.iscoroutinefunction(coro):
                coro = asyncio.coroutine(coro)
            if append and source is None:
                self.append_tier(coro, *args, **kwargs)
            else:
                self.add_tier(coro, *args, source=source, **kwargs)
            return coro
        return decorator

    def cleaner(self, coro):
        """ Function decorator for a cleanup cororoutine. """
        if not asyncio.iscoroutinefunction(coro):
            coro = asyncio.coroutine(coro)
        self.add_cleaner(coro)
        return coro

    def finalize(self):
        """ Look at our tiers and setup the final data flow.  Once this is run
        a cell can not be modified again. """
        self.assertNotFinalized()
        starters = []
        finishers = []
        for x in self.tiers:
            if not x.sources:
                starters.append(x)
            if not x.dests:
                finishers.append(x)
        self.add_tier(self.output_feed, source=finishers)
        self.coord.setup_wrap(self)
        self.finalized = True
        return starters

    @asyncio.coroutine
    def output_feed(self, route, *args):
        """ Simplify arguments and store them in the `output` buffer for
        yielding to the user. """
        self.output_buffer.extend(args)

    def loop_exception_handler(self, loop, context):
        exc = context.get('exception')
        if exc:
            if not self.pending_exception:
                self.pending_exception = exc
        elif self.loop_exception_handler_save:
            return self.loop_exception_handler_save(loop, context)
        else:
            return self.loop.default_exception_handler(context)

    def output(self):
        """ Produce a classic generator for this cell's final results. """
        starters = self.finalize()
        try:
            yield from self._output(starters)
        finally:
            self.close()

    def event_loop(self):
        """ Run the event loop once. """
        self.loop._thread_id = threading.get_ident()
        try:
            self.loop._run_once()
        finally:
            self.loop._thread_id = None

    def _output(self, starters):
        for x in starters:
            self.loop.create_task(x.enqueue_task(None))
        while True:
            while self.output_buffer:
                yield self.output_buffer.popleft()
            if not self.done():
                with self.loop_policy:
                    self.event_loop()
                if self.pending_exception:
                    exc = self.pending_exception
                    self.pending_exception = None
                    try:
                        raise exc
                    finally:
                        del exc
            else:
                flushed = False
                for t in self.tiers:
                    if t.buffer:
                        self.loop.create_task(t.flush())
                        flushed = True
                if not flushed and not self.output_buffer:
                    break
        with self.loop_policy:
            self.loop.run_until_complete(self.clean())

    @asyncio.coroutine
    def clean(self):
        """ Run all of the cleaners added by the user. """
        if self.cleaners:
            yield from asyncio.wait([x() for x in self.cleaners],
                                    loop=self.loop)

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.coord.close_wrap()
        self.cleanup_event_loop()
        for x in self.tiers:
            x.close()
        self.tiers = None
        self.tiers_coro_map = None
        self.cleaners = None
        self.coord = None

    def __iter__(self):
        return self.output()

    def __del__(self):
        self.close()
