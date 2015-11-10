"""
Coordinators.
"""


class CellCoordinator(object):
    """ Base class for IOCell coordinator.  These act as sorters and workflow
    managers for an IOCell.  They prioritize new requests and ensure that
    a particular io strategy is kept, such as maintaining low latency of final
    emitters, or ensuring balanced requests amongst levels, whatever the user
    may desire. """

    name = None

    def add_task(queue, task):
        raise NotImplementedError('required by subclass')


class FiFoCellCoordinator(CellCoordinator):
    """ Do not adjust the workflow at all, just enqueue new tasks as soon
    as we are asked to. """

    name = 'fifo'

    def add_task(queue, task):
        queue.add(task)


class LatencyCellCoordinator(CellCoordinator):

    name = 'latency'


coordinators = {
    LatencyCellCoordinator.name: LatencyCellCoordinator,
    FiFoCellCoordinator.name: FiFoCellCoordinator
}
