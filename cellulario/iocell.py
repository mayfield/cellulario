"""
A cell (biology) like bounding of an event loop.  An IOCell contains its own
event loop and provides some simplified entry and exit points for doing spurts
of async operations in a classical (blocking) system.  The cells can
communicate to each other as well for, `async_things | more_async_things`
flows.

This is sort of like loop.run_until_complete on steroids.  It tracks a cluster
of async operations and allows them to emit values to the user as they are made
available.  See the `Tier` code for a detailed explanation.
"""

import asyncio
import collections
import warnings
from . import coordination, tier

DEBUG = False

if hasattr(asyncio, 'all_tasks'):
    # python3.7+
    all_tasks=asyncio.all_tasks
else:
    all_tasks=asyncio.Task.all_tasks


class IOCellEventLoopPolicy(type(asyncio.get_event_loop_policy())):
    """ This event loop policy is used during the context of an IOCell
    operation to ensure calls to get_event_loop return the IOCell's event
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
        raise RuntimeError('Setting event loop in IOCell context is invalid')

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
    internally uses an async event loop to coordinate concurrent jobs.
    The jobs may be used to to cause further activity on the output stream.
    That is, the initial jobs may seed the next batch of jobs.

    Think of this as a portable IO loop that's wrapped up and managed for the
    context of a single generator to the outside world.  The calling code
    will work in normal blocking style. """

    Tier = tier.Tier

    def __init__(self, coord='noop', debug=DEBUG):
        if isinstance(coord, coordination.AbstractCellCoordinator):
            self.coord = coord
        else:
            self.coord = self.make_coord(coord)
        self.debug = debug
        self.output_buffer = collections.deque()
        self.pending_exception = None
        self.closed = False
        self.interrupted = False
        self.tiers = []
        self.tiers_job_map = {}
        self.cleaners = []
        self.finalized = False
        self.init_event_loop()
        self.output_event = asyncio.Event(loop=self.loop)

    def init_event_loop(self):
        """ Every cell should have its own event loop for proper containment.
        The type of event loop is not so important however. """
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(self.debug)
        self.loop_policy = IOCellEventLoopPolicy(self.loop)
        self.loop_exc_handler_save = self.loop.get_exception_handler()
        self.loop.set_exception_handler(self.loop_exception_handler)

    def cleanup_event_loop(self):
        """ Cleanup an event loop and close it down forever. """
        for task in all_tasks(loop=self.loop):
            if not task.done():
                if self.debug and not self.interrupted:
                    warnings.warn('Cancelling task: %s' % task)
                else:
                    task._log_destroy_pending = False
                task.cancel()
        with self.loop_policy:
            self.loop.run_until_complete(self.loop_iteration(timeout=0))
        self.loop.close()
        self.loop.set_exception_handler(self.loop_exc_handler_save)
        self.loop_exc_handler_save = None
        self.loop_policy = None
        self.loop = None

    def make_coord(self, name):
        return coordination.coordinators[name]()

    def done(self):
        return all(x.done() for x in all_tasks(loop=self.loop))

    def assertNotFinalized(self):
        """ Ensure the cell is not used more than once. """
        if self.finalized:
            raise RuntimeError('Already finalized: %s' % self)

    def add_tier(self, job, **kwargs):
        """ Add a job to the cell as a tier layer.  The source can be a single
        value or a list of either `Tier` types or coroutine functions already
        added to a `Tier` via `add_tier`. """
        assert asyncio.iscoroutinefunction(job)
        self.assertNotFinalized()
        tier = self.Tier(self, job, **kwargs)
        self.tiers.append(tier)
        self.tiers_job_map[job] = tier
        return tier

    def append_tier(self, job, **kwargs):
        """ Implicitly source from the tail tier like a pipe. """
        source = self.tiers[-1] if self.tiers else None
        return self.add_tier(job, source=source, **kwargs)

    def add_cleaner(self, job):
        """ Add a job to run after the cell is done.  This is for the user to
        perform any cleanup such as closing sockets. """
        assert asyncio.iscoroutinefunction(job)
        self.assertNotFinalized()
        self.cleaners.append(job)

    def tier(self, *args, append=True, source=None, **kwargs):
        """ Function decorator for a tier job.  The wrapped function must be
        async. """
        if len(args) == 1 and not kwargs and callable(args[0]):
            raise TypeError('Uncalled decorator syntax is invalid')

        def decorator(job):
            if append and source is None:
                self.append_tier(job, *args, **kwargs)
            else:
                self.add_tier(job, *args, source=source, **kwargs)
            return job
        return decorator

    def cleaner(self, job):
        """ Function decorator for a cleanup job. """
        self.add_cleaner(job)
        return job

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

    async def output_feed(self, route, *args):
        """ Simplify arguments and store them in the `output` buffer for
        yielding to the user. """
        self.output_buffer.extend(args)
        self.output_event.set()

    def loop_exception_handler(self, loop, context):
        self.output_event.set()
        exc = context.get('exception')
        if exc:
            if not self.pending_exception:
                self.pending_exception = exc
        elif self.loop_exc_handler_save:
            return self.loop_exc_handler_save(loop, context)
        else:
            return self.loop.default_exception_handler(context)

    def output(self):
        """ Produce a classic generator for this cell's final results. """
        starters = self.finalize()
        try:
            yield from self._output(starters)
        except (KeyboardInterrupt, GeneratorExit):
            self.interrupted = True
            raise
        finally:
            with self.loop_policy:
                self.loop.run_until_complete(self.clean())
            self.close()

    async def loop_iteration(self, timeout=0.01):
        """ XXX Hack timeout until all edge cases handled. """
        try:
            await asyncio.wait_for(self.output_event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        else:
            self.output_event.clear()

    def _output(self, starters):
        for x in starters:
            self.loop.create_task(x.enqueue_job(None))
        while True:
            while self.output_buffer:
                yield self.output_buffer.popleft()
            if not self.done():
                with self.loop_policy:
                    self.loop.run_until_complete(self.loop_iteration())
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

    async def clean(self):
        """ Run all of the cleaners added by the user. """
        if self.cleaners:
            await asyncio.wait([x() for x in self.cleaners], loop=self.loop)

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.finalized:
            self.coord.close_wrap()
        self.cleanup_event_loop()
        for x in self.tiers:
            x.close()
        self.tiers = None
        self.tiers_job_map = None
        self.cleaners = None
        self.coord = None

    def __iter__(self):
        return self.output()

    def __del__(self):
        self.close()
