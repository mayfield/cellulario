"""
Tier class.
"""

import asyncio
import collections
import weakref
from collections import abc


Route = collections.namedtuple('Route', 'source, cell, spec, emit')


class Tier(object):
    """ A managed layer of IO operations.  Generally speaking a tier should
    consist of one or more IO operations that are of consistent complexity
    with a single emit data type.  A tier emits data instead of returning it.
    Emissions flow from one tier to the next set of tiers until the final
    tiers are reached.

    A tier operates as a node in a directed acyclic graph (DAG).  Tiers are
    linked together by sourcing other tiers and are considered final when they
    are not sourced by any other tier.  A final tier or the HEAD tier in SCM
    terms can emit values to the consumer of the IOCell itself.  For example,
    if T1 emits v1 and T2 sources T1 then the job associated with T2 will be
    run with an argument of v1.  The emissions of T2 would be available to
    the cell user.  Example:

        >>> @cell.tier()
        ... async def T1(route):
        ...     await route.emit(1)

        >>> @cell.tier(source=T2)
        ... async def T2(route, v1):
        ...     await route.emit(v1 * 2)

        >>> print(list(cell))
        [2]

    There are different modes of operation with respect to sourcing and
    emitting.  A tier may source from more than one tier and the default mode
    is to simply run a tier's job for each emission of its source tiers.  An
    alternative mode is to gather the emissions from several source tiers and
    group them by a unique key.  When emissions from all the source tiers have
    been gathered for a particular grouping key, the job will be run with an
    argument list featuring all the relevant emit values.  It is a sort of
    micro variant of map-reduce.

    The tier's `job` is done by a user defined `asyncio.coroutine` which is
    automatically managed by the `IOCell` and `Coordinator` classes.  The
    `spec` attributes given by `IOCell.add_tier` are used to train the
    coordinator.  For example, the spec may indicate a concurrency factor or
    buffering min/max values.

    A tier contains routing information used to control how emit() calls flow.
    The default mode for emit is broadcast style signal emission to any other
    tiers that source from this tier.  Alternatively the tier can be configured
    to buffer emissions until a sufficient number of emissions are available.
    The actual number of emit values buffered is controlled by the coordinator.
    """

    job_tier_map = weakref.WeakValueDictionary()

    @classmethod
    def make_gatherer(cls, cell, source_tiers, gatherby):
        """ Produce a single source tier that gathers from a set of tiers when
        the key function returns a unique result for each tier. """
        pending = collections.defaultdict(dict)
        tier_hashes = [hash(x) for x in source_tiers]

        async def organize(route, *args):
            srchash = hash(route.source)
            key = gatherby(*args)
            group = pending[key]
            assert srchash not in group
            group[srchash] = args
            if len(group) == len(tier_hashes):
                del pending[key]
                await route.emit(*[group[x] for x in tier_hashes])
        return cls(cell, organize)

    def __init__(self, cell, job, source=None, buffer=0, gatherby=None,
                 **spec):
        if not asyncio.iscoroutinefunction(job):
            raise TypeError('`job` must be a coroutine')
        self.job = job
        self.job_tier_map[job] = self
        self.closed = False
        self.cell = cell
        self.sources = []
        self.dests = []
        if source:
            if not isinstance(source, abc.Sequence):
                source = [source]
            source_tiers = []
            for x_source in source:
                if not isinstance(x_source, Tier):
                    x_source = self.job_tier_map[x_source]
                source_tiers.append(x_source)
            if gatherby is not None:
                gatherer = self.make_gatherer(cell, source_tiers, gatherby)
                for x in source_tiers:
                    gatherer.add_source(x)
                self.add_source(gatherer)
            else:
                for x in source_tiers:
                    self.add_source(x)
        self.spec = spec
        self.buffer_max_size = buffer
        self.buffer = [] if buffer != 0 else None

    def __repr__(self):
        job_name = self.job and self.job.__name__
        return '<%s at 0x%x for %s, sources: %d, dests: %d, closed: %s>' % \
            (type(self).__name__, id(self), job_name, len(self.sources),
             len(self.dests), self.closed)

    async def enqueue_job(self, source, *args):
        """ Enqueue a user's job for execution in the background
        as soon as the coordinator clears it to do so. """
        await self.cell.coord.enqueue(self)
        route = Route(source, self.cell, self.spec, self.emit)
        self.cell.loop.create_task(self.job_runner(route, *args))
        # To guarantee that the event loop works fluidly, we manually yield
        # once. The coordinator enqueue coroutine is not required to yield so
        # this ensures we avoid various forms of event starvation.
        await asyncio.sleep(0)

    async def job_runner(self, *args):
        """ Run the job with coordination throttles wrapped around it. """
        await self.cell.coord.start(self)
        await self.job(*args)
        await self.cell.coord.finish(self)

    async def emit(self, *args):
        """ Send data to the next tier(s).  This call can be delayed if the
        coordinator thinks the backlog is too high for any of the emit
        destinations.  Likewise when buffering emit values prior to enqueuing
        them we ask the coordinator if we should flush the buffer each time in
        case the coordinator is managing the buffering by other metrics such
        as latency. """
        if self.buffer is not None:
            self.buffer.extend(args)
            if self.buffer_max_size is not None:
                flush = len(self.buffer) >= self.buffer_max_size
            else:
                flush = await self.cell.coord.flush(self)
            if flush:
                await self.flush()
        else:
            for t in self.dests:
                await t.enqueue_job(self, *args)

    async def flush(self):
        """ Flush the buffer of buffered tiers to our destination tiers. """
        if self.buffer is None:
            return
        data = self.buffer
        self.buffer = []
        for x in self.dests:
            await x.enqueue_job(self, *data)

    def add_source(self, tier):
        """ Schedule this tier to be called when another tier emits. """
        tier.add_dest(self)
        self.sources.append(tier)

    def add_dest(self, tier):
        """ Send data to this tier when we emit. """
        self.dests.append(tier)

    def close(self):
        """ Free any potential cycles. """
        self.cell = None
        self.job = None
        self.buffer = None
        del self.dests[:]
        del self.sources[:]
