"""
Coordinators act as sorters and workflow managers for an IOCell.  They
prioritize new requests and ensure that a particular IO strategy is kept, such
as maintaining low latency of final emitters or ensuring balanced requests
amongst levels.  The actual behavior may vary from one type to another.
"""

import asyncio


class AbstractCellCoordinator(object):
    """ Interface for cell coordinators. """

    name = None

    def setup_wrap(self, cell):
        return self.setup(cell)

    def close_wrap(self):
        return self.close()

    def setup(self, cell):
        """ Runs during IOCell.finalize. """
        pass

    def close(self):
        """ Perform any cleanup following a cell lifecycle completion. """
        pass

    async def flush(self, tier):
        """ A regulator for buffered tiers.  Should return True to instruct
        the source tier to flush it's buffered results to the destination
        tiers. """
        pass

    async def enqueue(self, tier):
        """ Subclasses should pause here to prevent enqueuing more jobs.  Note
        that this means the source tier is the code to be blocked by this. """
        pass

    async def start(self, tier):
        """ Subclasses should pause here if a tier is not permitted to start
        yet. """
        pass

    async def finish(self, tier):
        """ Subclasses should pause here if a tier is not permitted to finish
        yet. """
        pass


class NoopCoordinator(AbstractCellCoordinator):
    """ No-operation cell coordinator.  Nothing is blocked or altered. """

    name = 'noop'

    async def flush(self, tier):
        """ Disable buffering by always instructing a flush to happen. """
        return True


class PoolCellCoordinator(AbstractCellCoordinator):
    """ Regulate each tier as a pooled resource. """

    name = 'pool'

    def setup(self, cell):
        self.pools = {}
        for tier in cell.tiers:
            size = tier.spec.get('pool_size')
            if size is None:
                continue
            elif size == 0:
                raise ValueError('Pool size of 0 is invalid: %s' % tier)
            else:
                self.pools[tier] = asyncio.Semaphore(size, loop=cell.loop)

    def close(self):
        self.pools.clear()

    async def start(self, tier):
        sem = self.pools.get(tier)
        if sem is None:
            return
        await sem.acquire()

    async def finish(self, tier):
        sem = self.pools.get(tier)
        if sem is None:
            return
        sem.release()


coordinators = {
    PoolCellCoordinator.name: PoolCellCoordinator,
    NoopCoordinator.name: NoopCoordinator
}
