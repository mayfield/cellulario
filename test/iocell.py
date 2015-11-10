"""
AsyncIOGenerator tests.
"""

import asyncio
import logging
import unittest
from cellulario import IOCell

logging.basicConfig(level=0)


class TestIOCellNoFinal(unittest.TestCase):

    def test_init(self):
        cell = IOCell()
        self.assertFalse(list(cell))

    def test_one_tier_no_emit(self):
        cell = IOCell()
        refcnt = 0
        def f(tier):
            nonlocal refcnt
            refcnt += 1
        cell.add_tier(f)
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers_no_emit(self):
        cell = IOCell()
        refcnt = 0
        def f(tier):
            nonlocal refcnt
            refcnt += 1
        def f2(tier):
            nonlocal refcnt
            refcnt += 1
        t1 = cell.add_tier(f)
        cell.add_tier(f2, source=t1)
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers(self):
        cell = IOCell()
        def f(tier):
            tier.emit(123)
        def f2(tier, foo):
            self.assertEqual(foo, 123)
        t1 = cell.add_tier(f)
        cell.add_tier(f2, source=[t1])
        self.assertFalse(list(cell))


class TestIOCellWithFinal(unittest.TestCase):

    def tier_decor(self):
        return lambda fn: fn

    def test_single_final(self):
        cell = IOCell()
        refcnt = 0
        @self.tier_decor()
        def f(tier):
            nonlocal refcnt
            refcnt += 1
            tier.emit(123)
        cell.add_tier(f)
        results = list(cell)
        self.assertEqual(refcnt, 1)
        self.assertEqual(results, [123])

    def test_multi_emitter_1level(self):
        cell = IOCell()
        @self.tier_decor()
        def f(tier):
            tier.emit(123)
            tier.emit(321)
        cell.add_tier(f)
        self.assertEqual(list(cell), [123, 321])

    def test_multi_emitter_2level(self):
        cell = IOCell()
        @self.tier_decor()
        def f(tier):
            tier.emit(123)
            tier.emit(321)
        t = cell.add_tier(f)
        @self.tier_decor()
        def f2(tier, number):
            tier.emit(-number)
            tier.emit(number + 1)
        cell.add_tier(f2, source=t)
        self.assertEqual(list(cell), [-123, 124, -321, 322])


class TestIOCellWithFinalCoro(TestIOCellWithFinal):

    def tier_decor(self):
        return asyncio.coroutine


class TestIOCellExceptions(unittest.TestCase):

    def tier_decor(self):
        return lambda fn: fn

    def test_blowup(self):
        cell = IOCell()
        @self.tier_decor()
        def f(tier):
            raise RuntimeError()
        cell.add_tier(f)
        self.assertRaises(RuntimeError, list, cell)

    def test_multi_blowup(self):
        cell = IOCell()
        @self.tier_decor()
        def f(tier):
            raise RuntimeError()
        cell.add_tier(f)
        @self.tier_decor()
        def f2(tier):
            raise ValueError()
        cell.add_tier(f2)
        it = iter(cell)
        self.assertRaises(RuntimeError, next, it)
        self.assertRaises(StopIteration, next, it)  # The value error is just dropped.


class TestIOCellExceptionsCoro(TestIOCellExceptions):

    def tier_decor(self):
        return asyncio.coroutine


class TestIOCellCoroBasics(unittest.TestCase):

    @asyncio.coroutine
    def add(self, *args):
        return sum(args)

    def test_yield_from(self):
        cell = IOCell()
        @asyncio.coroutine
        def coro(tier):
            tier.emit((yield from self.add(2, 3)))
        cell.add_tier(coro)
        self.assertEqual(list(cell), [5])
