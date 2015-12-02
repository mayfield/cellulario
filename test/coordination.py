"""
Correctness testing for coordination.
"""

import asyncio
import time
import unittest
from cellulario import IOCell
from cellulario.coordination import AbstractCellCoordinator



class InterfaceTests(unittest.TestCase):

    def test_enter_exit_bookends(self):
        states = {}
        class Coord(AbstractCellCoordinator):
            @asyncio.coroutine
            def start(self, tier):
                states['enter'] = time.time()
            @asyncio.coroutine
            def finish(self, tier):
                states['exit'] = time.time()
        cell = IOCell(coord=Coord(), debug=True)
        @cell.tier()
        def t(route):
            states['proc'] = time.time()
        list(cell)
        self.assertSetEqual(set(states), {'enter', 'proc', 'exit'})
        self.assertTrue(states['enter'] < states['proc'] < states['exit'])
