"""
AsyncIOGenerator tests.
"""

import asyncio
import unittest
from cellulario import IOCell as _IOCell
from unittest import mock


def IOCell(*args, debug=True, **kwargs):
    return _IOCell(*args, debug=debug, **kwargs)


class NoFinal(unittest.TestCase):

    def test_init(self):
        cell = IOCell()
        self.assertFalse(list(cell))

    def test_one_tier_no_emit(self):
        cell = IOCell()
        refcnt = 0
        @asyncio.coroutine
        def f(tier):
            nonlocal refcnt
            refcnt += 1
        cell.add_tier(f)
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers_no_emit(self):
        cell = IOCell()
        refcnt = 0
        @asyncio.coroutine
        def f(tier):
            nonlocal refcnt
            refcnt += 1
        @asyncio.coroutine
        def f2(tier):
            nonlocal refcnt
            refcnt += 1
        t1 = cell.add_tier(f)
        cell.add_tier(f2, source=t1)
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers(self):
        cell = IOCell()
        @asyncio.coroutine
        def f(tier):
            yield from tier.emit(123)
        @asyncio.coroutine
        def f2(tier, foo):
            self.assertEqual(foo, 123)
        t1 = cell.add_tier(f)
        cell.add_tier(f2, source=[t1])
        self.assertFalse(list(cell))


class WithFinal(unittest.TestCase):

    def test_single_final(self):
        cell = IOCell()
        refcnt = 0
        @asyncio.coroutine
        def f(tier):
            nonlocal refcnt
            refcnt += 1
            yield from tier.emit(123)
        cell.add_tier(f)
        results = list(cell)
        self.assertEqual(refcnt, 1)
        self.assertEqual(results, [123])

    def test_multi_emitter_1level(self):
        cell = IOCell()
        @asyncio.coroutine
        def f(tier):
            yield from tier.emit(123)
            yield from tier.emit(321)
        cell.add_tier(f)
        self.assertEqual(list(cell), [123, 321])

    def test_multi_emitter_2level(self):
        cell = IOCell()
        @asyncio.coroutine
        def f(tier):
            yield from tier.emit(123)
            yield from tier.emit(321)
        t = cell.add_tier(f)
        @asyncio.coroutine
        def f2(tier, number):
            yield from tier.emit(-number)
            yield from tier.emit(number + 1)
        cell.add_tier(f2, source=t)
        self.assertEqual(list(cell), [-123, 124, -321, 322])

    def test_multi_emitter_multi_source(self):
        cell = IOCell()

        @asyncio.coroutine
        def a1(tier):
            yield from tier.emit('a1-1')
            yield from tier.emit('a1-2')
        a1t = cell.add_tier(a1)

        @asyncio.coroutine
        def a2(tier):
            yield from tier.emit('a2-1')
            yield from tier.emit('a2-2')
        a2t = cell.add_tier(a2)

        @asyncio.coroutine
        def b(tier, value):
            yield from tier.emit(value)
        cell.add_tier(b, source=[a1t, a2t])

        self.assertEqual(list(cell), ['a1-1', 'a1-2', 'a2-1', 'a2-2'])


class Exceptions(unittest.TestCase):

    def test_blowup(self):
        cell = IOCell()
        @asyncio.coroutine
        def f(tier):
            raise RuntimeError()
        cell.add_tier(f)
        self.assertRaises(RuntimeError, list, cell)

    def test_multi_blowup(self):
        cell = IOCell()
        @asyncio.coroutine
        def f(tier):
            raise RuntimeError()
        cell.add_tier(f)
        @asyncio.coroutine
        def f2(tier):
            raise ValueError()
        cell.add_tier(f2)
        it = iter(cell)
        self.assertRaises(RuntimeError, next, it)
        self.assertRaises(StopIteration, next, it)  # The value error is just dropped.


class CoroBasics(unittest.TestCase):

    @asyncio.coroutine
    def add(self, *args):
        return sum(args)

    def test_yield_from(self):
        cell = IOCell()
        @asyncio.coroutine
        def coro(tier):
            yield from tier.emit((yield from self.add(2, 3)))
        cell.add_tier(coro)
        self.assertEqual(list(cell), [5])


class ShortPatterns(unittest.TestCase):

    def test_one_tier_no_emit(self):
        cell = IOCell()
        refcnt = 0
        @cell.tier()
        @asyncio.coroutine
        def f(tier):
            nonlocal refcnt
            refcnt += 1
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers_no_emit_tier_deco(self):
        cell = IOCell()
        refcnt = 0
        @cell.tier()
        @asyncio.coroutine
        def f(tier):
            nonlocal refcnt
            refcnt += 1
        @cell.tier(source=f)
        @asyncio.coroutine
        def f2(tier):
            nonlocal refcnt
            refcnt += 1
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers_no_emit_tier_coro_deco(self):
        cell = IOCell()
        refcnt = 0
        @cell.tier_coroutine()
        def f(tier):
            nonlocal refcnt
            refcnt += 1
        @cell.tier_coroutine(source=f)
        def f2(tier):
            nonlocal refcnt
            refcnt += 1
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)


class Nesting(unittest.TestCase):

    def test_from_cell_generator(self):
        inner = IOCell()
        @inner.tier_coroutine()
        def inner_tier(tier):
            for i in range(3):
                yield from tier.emit(i)
        outer = IOCell()
        @outer.tier_coroutine()
        def outer_tier(tier):
            yield from asyncio.sleep(0)
            inner_it = iter(inner)
            for i in range(3):
                self.assertEqual(i, next(inner_it))
        list(outer)


class Misuse(unittest.TestCase):

    def test_uncalled_tier_decor(self):
        cell = IOCell()
        def setup():
            @cell.tier
            def fn(tier):
                pass
        self.assertRaises(TypeError, setup)

    def test_uncalled_tier_coro_decor(self):
        cell = IOCell()
        def setup():
            @cell.tier_coroutine
            def fn(tier):
                pass
        self.assertRaises(TypeError, setup)

    def test_miscycle_reset(self):
        cell = IOCell()
        @cell.tier_coroutine()
        def fn(tier):
            for i in range(3):
                yield from tier.emit(i)
        it = iter(cell)
        next(it)
        self.assertRaises(RuntimeError, cell.reset)


class LifeCycle(unittest.TestCase):

    def test_no_reuse(self):
        cell = IOCell()
        list(cell)
        self.assertRaises(RuntimeError, list, cell)

    def test_reuse_after_reset(self):
        cell = IOCell()
        @cell.tier_coroutine()
        def coro(tier):
            for i in range(3):
                yield from tier.emit(i)
        self.assertEqual(list(cell), list(range(3)))
        cell.reset()
        self.assertEqual(list(cell), list(range(3)))

    def test_trailing_work(self):
        cell = IOCell()
        fullrun = False
        @cell.tier_coroutine()
        def coro(tier):
            nonlocal fullrun
            for i in range(3):
                yield from tier.emit(i)
            for ii in range(3):
                yield from asyncio.sleep(0)
            fullrun = True
        self.assertEqual(list(cell), list(range(3)))
        self.assertTrue(fullrun)

    def test_background_work_clean_exit(self):
        cell = IOCell()
        loop = cell.loop
        cancel = mock.Mock()
        @asyncio.coroutine
        def bg():
            yield from asyncio.sleep(1e300)
        @cell.tier_coroutine()
        def coro(tier):
            task = loop.create_task(bg())
            task.cancel = cancel
        self.assertEqual(list(cell), [])
        self.assertTrue(cancel.called)

    def test_background_work_exception(self):
        cell = IOCell()
        loop = cell.loop
        cancel = mock.Mock()
        @asyncio.coroutine
        def bg():
            yield from asyncio.sleep(1e300)
        @cell.tier_coroutine()
        def coro(tier):
            task = loop.create_task(bg())
            task.cancel = cancel
            raise Exception()
        self.assertRaises(Exception, list, cell)
        self.assertTrue(cancel.called)

    def test_background_work_gc(self):
        cell = IOCell()
        loop = cell.loop
        cancel = mock.Mock()
        @asyncio.coroutine
        def bg():
            yield from asyncio.sleep(1e300)
        @cell.tier_coroutine()
        def coro(tier):
            task = loop.create_task(bg())
            task.cancel = cancel
            for i in range(10):
                yield from tier.emit(i)
        l = iter(cell)
        next(l)
        self.assertFalse(cancel.called)
        del l
        self.assertTrue(cancel.called)
