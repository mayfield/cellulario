"""
AsyncIOGenerator tests.
"""

import asyncio
import operator
import unittest
from cellulario import IOCell


class NoFinal(unittest.TestCase):

    def test_init(self):
        cell = IOCell()
        self.assertFalse(list(cell))

    def test_one_tier_no_emit(self):
        cell = IOCell()
        refcnt = 0
        async def f(route):
            nonlocal refcnt
            refcnt += 1
        cell.add_tier(f)
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers_no_emit(self):
        cell = IOCell()
        refcnt = 0
        async def f(route):
            nonlocal refcnt
            refcnt += 1
        async def f2(route):
            nonlocal refcnt
            refcnt += 1
        t1 = cell.add_tier(f)
        cell.add_tier(f2, source=t1)
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers(self):
        cell = IOCell()
        async def f(route):
            await route.emit(123)
        async def f2(route, foo):
            self.assertEqual(foo, 123)
        t1 = cell.add_tier(f)
        cell.add_tier(f2, source=[t1])
        self.assertFalse(list(cell))

    def test_varargs(self):
        cell = IOCell()
        async def f(route):
            await route.emit(123, 345)
        async def f2(route, foo, bar):
            self.assertEqual(foo, 123)
            self.assertEqual(bar, 345)
        t1 = cell.add_tier(f)
        cell.add_tier(f2, source=[t1])
        self.assertFalse(list(cell))


class WithFinal(unittest.TestCase):

    def test_single_final(self):
        cell = IOCell()
        refcnt = 0
        async def f(route):
            nonlocal refcnt
            refcnt += 1
            await route.emit(123)
        cell.add_tier(f)
        results = list(cell)
        self.assertEqual(refcnt, 1)
        self.assertEqual(results, [123])

    def test_multi_emitter_1level(self):
        cell = IOCell()
        async def f(route):
            await route.emit(123)
            await route.emit(321)
        cell.add_tier(f)
        self.assertEqual(list(cell), [123, 321])

    def test_multi_emitter_2level(self):
        cell = IOCell()
        async def f(route):
            await route.emit(123)
            await route.emit(321)
        t = cell.add_tier(f)
        async def f2(route, number):
            await route.emit(-number)
            await route.emit(number + 1)
        cell.add_tier(f2, source=t)
        self.assertEqual(list(cell), [-123, 124, -321, 322])

    def test_multi_emitter_multi_source(self):
        cell = IOCell()
        async def a1(route):
            await route.emit('a1-1')
            await route.emit('a1-2')
        a1t = cell.add_tier(a1)
        async def a2(route):
            await route.emit('a2-1')
            await route.emit('a2-2')
        a2t = cell.add_tier(a2)
        async def b(route, value):
            await route.emit(value)
        cell.add_tier(b, source=[a1t, a2t])
        self.assertEqual(list(cell), ['a1-1', 'a2-1', 'a1-2', 'a2-2'])

    def test_varargs(self):
        cell = IOCell()
        async def f(route):
            await route.emit(*'abc')
            await route.emit(*'def')
        cell.add_tier(f)
        self.assertEqual(list(cell), list('abcdef'))

    def test_multi_emitter_1level(self):
        cell = IOCell()
        async def f(route):
            await route.emit(123)
            await route.emit(321)
        cell.add_tier(f)
        self.assertEqual(list(cell), [123, 321])

    def test_multi_emitter_2level(self):
        cell = IOCell()
        async def f(route):
            await route.emit(123)
            await route.emit(321)
        t = cell.add_tier(f)
        async def f2(route, number):
            await route.emit(-number)
            await route.emit(number + 1)
        cell.add_tier(f2, source=t)
        self.assertEqual(list(cell), [-123, 124, -321, 322])

    def test_multi_emitter_multi_source(self):
        cell = IOCell()
        async def a1(route):
            await route.emit('a1-1')
            await route.emit('a1-2')
        a1t = cell.add_tier(a1)
        async def a2(route):
            await route.emit('a2-1')
            await route.emit('a2-2')
        a2t = cell.add_tier(a2)
        async def b(route, value):
            await route.emit(value)
        cell.add_tier(b, source=[a1t, a2t])
        self.assertEqual(list(cell), ['a1-1', 'a2-1', 'a1-2', 'a2-2'])


class Exceptions(unittest.TestCase):

    def test_blowup(self):
        cell = IOCell()
        async def f(route):
            raise RuntimeError()
        cell.add_tier(f)
        self.assertRaises(RuntimeError, list, cell)

    def test_multi_blowup(self):
        cell = IOCell()
        async def f(route):
            raise RuntimeError()
        cell.add_tier(f)
        async def f2(route):
            raise ValueError()
        cell.add_tier(f2)
        it = iter(cell)
        self.assertRaises(RuntimeError, next, it)
        self.assertRaises(StopIteration, next, it)  # ValueError is dropped.

    def test_background_work_exception(self):
        cell = IOCell()
        async def bg():
            raise Exception()
        @cell.tier()
        async def coro(route):
            cell.loop.create_task(bg())
            await asyncio.sleep(0)  # allow bg to run.
        self.assertRaises(Exception, list, cell)


class CoroBasics(unittest.TestCase):

    async def add(self, *args):
        return sum(args)

    def test_yield_from(self):
        cell = IOCell()
        async def coro(route):
            await route.emit((await self.add(2, 3)))
        cell.add_tier(coro)
        self.assertEqual(list(cell), [5])


class ShortPatterns(unittest.TestCase):

    def test_one_tier_no_emit(self):
        cell = IOCell()
        refcnt = 0
        @cell.tier()
        async def f(route):
            nonlocal refcnt
            refcnt += 1
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers_no_emit_tier_deco(self):
        cell = IOCell()
        refcnt = 0
        @cell.tier()
        async def f(route):
            nonlocal refcnt
            refcnt += 1
        @cell.tier(source=f)
        async def f2(route):
            nonlocal refcnt
            refcnt += 1
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_cascaded_tiers_no_emit_tier_coro_deco(self):
        cell = IOCell()
        refcnt = 0

        @cell.tier()
        async def f(route):
            nonlocal refcnt
            refcnt += 1

        @cell.tier(source=f)
        async def f2(route):
            nonlocal refcnt
            refcnt += 1
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)

    def test_source_from_coroutine_sequence(self):
        cell = IOCell()
        refcnt = 0

        @cell.tier()
        async def f(route):
            nonlocal refcnt
            refcnt += 1

        @cell.tier(source=[f])
        async def f2(route):
            nonlocal refcnt
            refcnt += 1
        self.assertFalse(list(cell))
        self.assertEqual(refcnt, 1)


class Nesting(unittest.TestCase):

    def test_from_cell_generator(self):
        inner = IOCell()

        @inner.tier()
        async def inner_tier(route):
            for i in range(3):
                await route.emit(i)
        outer = IOCell()

        @outer.tier()
        async def outer_tier(route):
            await asyncio.sleep(0)
            inner_it = iter(inner)
            for i in range(3):
                self.assertEqual(i, next(inner_it))
        # XXX not supported for now
        self.assertRaises(RuntimeError, list, outer)


class Misuse(unittest.TestCase):

    def test_uncalled_tier_decor(self):
        cell = IOCell()
        def setup():
            @cell.tier
            def fn(route):
                pass
        self.assertRaises(TypeError, setup)

    def test_uncalled_tier_coro_decor(self):
        cell = IOCell()
        def setup():
            @cell.tier
            def fn(route):
                pass
        self.assertRaises(TypeError, setup)


class LifeCycle(unittest.TestCase):

    def test_no_reuse(self):
        cell = IOCell()
        list(cell)
        self.assertRaises(RuntimeError, list, cell)

    def test_trailing_work(self):
        cell = IOCell()
        fullrun = False
        @cell.tier()
        async def coro(route):
            nonlocal fullrun
            for i in range(3):
                await route.emit(i)
            for ii in range(3):
                await asyncio.sleep(0)
            fullrun = True
        self.assertEqual(list(cell), list(range(3)))
        self.assertTrue(fullrun)

    def test_background_work_clean_exit(self):
        cell = IOCell()
        bg_done = False
        async def bg():
            nonlocal bg_done
            for x in range(100):
                await asyncio.sleep(0)
            bg_done = True
        @cell.tier()
        async def coro(route):
            route.cell.loop.create_task(bg())
        self.assertEqual(list(cell), [])
        self.assertTrue(bg_done)

    def test_background_work_gc(self):
        cell = IOCell()
        loop = cell.loop
        task = None
        async def bg():
            await asyncio.sleep(1<<20)
        @cell.tier()
        async def coro(route):
            nonlocal task
            task = loop.create_task(bg())
            await route.emit(1)
            await route.emit(2)
        l = iter(cell)
        next(l)
        self.assertFalse(task.done())
        del l
        self.assertTrue(task.cancelled())


class Buffering(unittest.TestCase):

    def test_buffer_1tier_2rem(self):
        cell = IOCell()
        @cell.tier(buffer=4)
        async def f(route):
            for i in range(10):
                await route.emit(i)
        self.assertEqual(list(cell), list(range(10)))

    def test_buffer_1tier_0rem(self):
        cell = IOCell()
        @cell.tier(buffer=2)
        async def f(route):
            for i in range(4):
                await route.emit(i)
        self.assertEqual(list(cell), list(range(4)))

    def test_buffer_2tier_0rem(self):
        cell = IOCell()
        cnt = 0
        @cell.tier(buffer=2)
        async def f(route):
            await route.emit(1)
            await route.emit(2)
            await route.emit(1)
            await route.emit(2)
        @cell.tier(source=f)
        async def f2(route, a, b):
            nonlocal cnt
            cnt += 1
            self.assertEqual(a, 1)
            self.assertEqual(b, 2)
            await route.emit(a)
            await route.emit(b)
        self.assertEqual(list(cell), [1, 2, 1, 2])
        self.assertEqual(cnt, 2)

    def test_buffer_1tier_2rem_varargs(self):
        cell = IOCell()
        @cell.tier(buffer=4)
        async def f(route):
            for i in range(10):
                await route.emit(i, str(i))
        expect = []
        for i in range(10):
            expect.extend((i, str(i)))
        self.assertEqual(list(cell), expect)


class Gather(unittest.TestCase):

    def test_gatherby_dict_key(self):
        cell = IOCell()
        reduced = False
        @cell.tier()
        async def a1(route):
            for i in range(10):
                await route.emit({
                    "foo": i,
                    "source": 'a1'
                })
        @cell.tier(append=False)
        async def a2(route):
            for i in range(9, -1, -1):
                await route.emit({
                    "foo": i,
                    "source": 'a2'
                })
        @cell.tier(source=[a1, a2], gatherby=operator.itemgetter('foo'))
        async def reducer(route, t1, t2):
            nonlocal reduced
            self.assertEqual(t1[0]['foo'], t2[0]['foo'])
            self.assertEqual(t1[0]['source'], 'a1')
            self.assertEqual(t2[0]['source'], 'a2')
            reduced = True
        list(cell)
        self.assertTrue(reduced)

    def test_gatherby_pos_arg(self):
        cell = IOCell()
        reduced = False
        @cell.tier()
        async def a1(route):
            await route.emit(111, 'first')
        @cell.tier(append=False)
        async def a2(route):
            await route.emit(111, 'second')
        @cell.tier(source=[a1, a2], gatherby=lambda a, b: a)
        async def reducer(route, t1, t2):
            nonlocal reduced
            self.assertEqual(t1[0], 111)
            self.assertEqual(t2[0], 111)
            self.assertEqual(t1[1], 'first')
            self.assertEqual(t2[1], 'second')
            reduced = True
        list(cell)
        self.assertTrue(reduced)


class AppendTier(unittest.TestCase):

    def test_one_tier_append(self):
        cell = IOCell()
        async def f(route):
            await route.emit(123)
        cell.append_tier(f)
        self.assertEqual(list(cell), [123])

    def test_one_tier_coro(self):
        cell = IOCell()
        @cell.tier()
        async def f(route):
            await route.emit(123)
        self.assertEqual(list(cell), [123])

    def test_two_tier_coro(self):
        cell = IOCell()
        @cell.tier()
        async def f(route):
            await route.emit(100)
        @cell.tier()
        async def f2(route, i):
            await route.emit(i + 50)
        self.assertEqual(list(cell), [150])

    def test_coro_noappend(self):
        cell = IOCell()
        @cell.tier()
        async def a1(route):
            await route.emit(100)
        @cell.tier(append=False)
        async def a2(route):
            await route.emit(200)
        self.assertEqual(list(cell), [100, 200])

    def test_coro_source_override(self):
        cell = IOCell()
        @cell.tier()
        async def a1(route):
            await route.emit(100)
        @cell.tier(append=False)
        async def a2(route):
            await route.emit(200)
        @cell.tier(source=a1)
        async def b1(route, i):
            await route.emit(i + 1)
        self.assertEqual(list(cell), [200, 101])
