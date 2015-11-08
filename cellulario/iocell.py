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
    are made available. """

    TaskTier = task_tier.TaskTier

    def __init__(self, coordinator='fifo'):
        self.coord = coordination.coordinators[coordinator]
        self.tiers = {}

    def add_task_tier(self, consumes=None):
        """ Create a new tier of task.  When these types of tasks are added
        to the work order they will be coordinated according to the specs and
        heuristics of this tier.  If this tier consumes another tier then
        results emitted from that tier will be used to automatically add new
        tasks of this type.  If this tier is not consumed by any others then
        it's results are effectively "public" to the IOCell consumer;  That
        is, they are made available to the output routines of a IOCell. """
        tier = self.TaskTier()
        if consumes is not None:
            consumes.subscribe(tier)
        return tier


class AsyncIOCell(IOCell):
    """ Produce a classic generator to the outside world that internally uses
    an async ioloop to coordinate concurrent tasks.  The tasks may be used to
    to cause further activity on the output stream.  That is the initial work
    orders may be tasks used to seed more work.

    Think of this as a portable ioloop that's wrapped up and managed for the
    context of a single generator to the outside world.  The calling code
    will work in a normal blocking fashion. """

    def __init__(self, loop=None, coordinator='latency', max_concurrency=None,
                 timeout=None):
        self.loop = loop or asyncio.get_event_loop()
        self.pending = 1
        self.results = collections.deque()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.pending:
                self.ioloop.start()
            else:
                break
            while self.results:
                yield self.results.popleft()
        try:
            return self._async_remote(ioloop, *args, **kwargs)
        finally:
            ioloop.stop()

    def add_emitter(self, consumes=None):
        pass
