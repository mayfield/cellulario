"""
Task tier.
"""


class TaskTier(object):
    """ A specific tier/type of cell operation.  Generally speaking a tier
    should be one or more IO operations that are equal in IO complexity. So
    while permissible to make multiple IO calls, they should not be
    interdependent or of different complexity. """

    def __init__(self):
        self.consumers = []
        self.pending = 0
