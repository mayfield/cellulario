"""
Coordinators.
"""

import asyncio


class CellCoordinator(object):
    """ Base class for IOCell coordinator.  These act as sorters and workflow
    managers for an IOCell.  They prioritize new requests and ensure that
    a particular io strategy is kept, such as maintaining low latency of final
    emitters, or ensuring balanced requests amongst levels, whatever the user
    may desire. """

    name = 'noop'

    def setup(self, tiers):
        """ Runs during IOCell.finalize. """
        pass

    @asyncio.coroutine
    def enqueue(self, tier):
        """ Subclasses should pause here to prevent enqueuing more work.  Note
        that this means the source tier is the code to be blocked by this. """
        pass

    @asyncio.coroutine
    def enter(self, tier):
        """ Subclasses should pause here if a tier is not permitted to start
        yet. """
        pass

    @asyncio.coroutine
    def exit(self, tier):
        """ Subclasses should pause here if a tier is not permitted to exit
        yet. """
        pass


class PoolCellCoordinator(CellCoordinator):
    """ Regulate each tier as a pooled resource. """

    name = 'pool'

    def setup(self, tiers):
        self.pools = {}
        for tier in tiers:
            size = tier.spec.get('pool_size')
            if size is None:
                continue
            elif size == 0:
                raise ValueError('Pool size of 0 is invalid: %s' % tier)
            else:
                self.pools[tier] = asyncio.Semaphore(size)

    @asyncio.coroutine
    def enter(self, tier):
        sem = self.pools.get(tier)
        if sem is None:
            return
        yield from sem.acquire()

    @asyncio.coroutine
    def exit(self, tier):
        sem = self.pools.get(tier)
        if sem is None:
            return
        sem.release()


class LatencyCellCoordinator(CellCoordinator):

    name = 'latency'


coordinators = {
    LatencyCellCoordinator.name: LatencyCellCoordinator,
    PoolCellCoordinator.name: PoolCellCoordinator,
    CellCoordinator.name: CellCoordinator
}
