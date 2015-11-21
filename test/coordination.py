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


class InterfaceTests(unittest.TestCase):

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
        cell = IOCell(coord=Coord(), debug=True)
        @cell.tier_coroutine()
        def t(tier):
            states['proc'] = time.time()
        list(cell)
        self.assertSetEqual(set(states), {'enter', 'proc', 'exit'})
        self.assertTrue(states['enter'] < states['proc'] < states['exit'])
