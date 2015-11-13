"""
Correctness testing for coordination.
"""

import asyncio
import logging
import time
import unittest
from cellulario import IOCell
from cellulario.coordination import CellCoordinator

logging.basicConfig(level=0)


class CoordTestBase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(True)

    def tearDown(self):
        self.loop.close()


class InterfaceTests(CoordTestBase):

    def test_enter_exit_bookends(self):
        states = {}
        class Coord(CellCoordinator):
            @asyncio.coroutine
            def enter(self, tier):
                states['enter'] = time.time()
                super().enter(tier)
            @asyncio.coroutine
            def exit(self, tier):
                states['exit'] = time.time()
                super().exit(tier)
        cell = IOCell(loop=self.loop, coord=Coord())
        @cell.tier_coroutine()
        def t(tier):
            states['proc'] = time.time()
        list(cell)
        self.assertSetEqual(set(states), {'enter', 'proc', 'exit'})
        self.assertTrue(states['enter'] < states['proc'] < states['exit'])
