"""
Tier class.
"""

import asyncio


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
    if T1 emits v1 and T2 sources T1 then the coroutine associated with T2 will
    be run with an argument of v1.  The emissions of T2 would be available to
    the cell user.  Example python 3.5 style code would be:

        >>> @cell.tier()
        ... async def T1(tier):
        ...     await tier.emit(1)

        >>> @cell.tier(source=T2)
        ... async def T2(tier, v1):
        ...     await tier.emit(v1 * 2)

        >>> print(list(cell))
        [2]

    The same example in python 3.4:

        >>> @cell.tier_coroutine()
        ... def T1(tier):
        ...     yeild from tier.emit(1)

        >>> @cell.tier_coroutine(source=T2)
        ... def T2(tier, v1):
        ...     yield from tier.emit(v1 * 2)

        >>> print(list(cell))
        [2]

    There are different modes of operation with respect to sourcing and
    emitting.  A tier may source from more than one tier and the default mode
    is to simply run a tier's coroutine for each emission of its source tiers.
    An alternative mode is to gather the emissions from several source tiers
    and group them by a unique key.  When emissions from all the source tiers
    have been gathered for a particular grouping key, the coroutine will be
    run with an argument list featuring all the relevant emit values.  It is a
    sort of micro variant of map-reduce.

    The work of a tier is done by a user defined `asyncio.coroutine` which is
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

    @property
    def target_count(self):
        return len(self.targets) if self.targets is not None else 0

    def __init__(self, cell, coro, buffer=0, **spec):
        self.closed = False
        self.cell = cell
        self.targets = []
        self.coro = coro
        self.spec = spec
        self.buffer_max_size = buffer
        self.buffer = [] if buffer != 0 else None
        if not asyncio.iscoroutinefunction(coro):
            raise ValueError("Function argument must be a coroutine")
        self.source_count = 0

    def __repr__(self):
        coro_name = self.coro and self.coro.__name__
        return '<TaskTier at 0x%x for %s, sources: %d, targets: %d, closed: ' \
            '%s>' % (id(self), coro_name, self.source_count, self.target_count,
            self.closed)

    @asyncio.coroutine
    def enqueue_task(self, *data):
        """ Enqueue a task execution.  It will run in the background as soon
        as the coordinator clears it to do so. """
        yield from self.cell.coord.enqueue(self)
        self.cell.loop.create_task(self.coord_wrap(*data))

    @asyncio.coroutine
    def coord_wrap(self, *data):
        """ Wrap the coroutine with coordination throttles. """
        yield from self.cell.coord.start(self)
        yield from self.coro(self, *data)
        yield from self.cell.coord.finish(self)

    @asyncio.coroutine
    def emit(self, datum):
        """ Send data to the next tier(s).  This call can be delayed if the
        coordinator thinks the backlog is too high for any of the emit
        targets.  Likewise when buffering emit values prior to enqueuing them
        we ask the coordinator if we should flush the buffer each time in case
        the coordinator is managing the buffering by other metrics such as
        latency. """
        if self.buffer is not None:
            self.buffer.append(datum)
            if self.buffer_max_size is not None:
                flush = len(self.buffer) >= self.buffer_max_size
            else:
                flush = yield from self.cell.coord.flush(self)
            if flush:
                yield from self.flush()
        else:
            for t in self.targets:
                yield from t.enqueue_task(datum)

    @asyncio.coroutine
    def flush(self):
        """ Flush the buffer of buffered tiers to our target tiers. """
        if self.buffer is None:
            return
        data = self.buffer
        self.buffer = []
        for t in self.targets:
            yield from t.enqueue_task(*data)

    def source_from(self, source_tier):
        """ Schedule this tier to be called when another tier emits. """
        self.source_count += 1
        source_tier.add_target(self)

    def add_target(self, tier):
        """ Run a callback when this tier emits data. """
        self.targets.append(tier)

    def close(self):
        """ Free any potential cycles. """
        self.coro = None
        self.targets = None
        self.cell = None
