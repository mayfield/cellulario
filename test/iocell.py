"""
AsyncIOGenerator tests.
"""

import unittest
from ecmcli.api import IOCell


class TestIOCell(unittest.TestCase):

    def test_sanity(self):
        cell = IOCell()
        self.assertFalse(list(cell))

    def test_tier_sanity(self):
        cell = IOCell()
        cell.add_task_tier()
        self.assertFalse(list(cell))

    def test_tier_used_sanity(self):
        cell = IOCell()

        @cell.add_task_tier()
        def f():
            pass
        self.assertFalse(list(cell))

    def test_simple_flow(self):
        cell = IOCell()
        tier = cell.add_task_tier()

        @tier
        def f(cell, task, source=None):
            task.set_result('foo')
