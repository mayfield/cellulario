"""
Coordinators.
"""

import asyncio
import logging
import warnings

logger = logging.getLogger('cio.coord')


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
            if not tier.spec or 'pool_size' not in tier.spec:
                warnings.warn('Tier without spec["pool_size"], defaulting to '
                              'size 1: %s' % tier)
                size = 1
            else:
                size = tier.spec['pool_size']
            self.pools[tier] = asyncio.Semaphore(size)

    @asyncio.coroutine
    def enter(self, tier):
        sem = self.pools[tier]
        logger.debug("Waiting for pool availability: %s" % sem)
        yield from sem.acquire()
        logger.debug("Acquired pool availability: %s" % sem)

    @asyncio.coroutine
    def exit(self, tier):
        sem = self.pools[tier]
        logger.debug("Releasing pool resource: %s" % sem)
        sem.release()


class LatencyCellCoordinator(CellCoordinator):

    name = 'latency'


coordinators = {
    LatencyCellCoordinator.name: LatencyCellCoordinator,
    PoolCellCoordinator.name: PoolCellCoordinator,
    CellCoordinator.name: CellCoordinator
}
