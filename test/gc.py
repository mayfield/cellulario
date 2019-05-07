"""
Garbage collection verification.

A design goal of cellulario is to avoid circular references so that cleanup
of cells is immediate.  This is partially a best practice and secondly good
for setting off the assertions in asyncio code that fire when their objects
are deleted.
"""

import asyncio
import gc
import unittest
import weakref
from cellulario import IOCell, Tier


class CollectWithoutGC(unittest.TestCase):

    def setUp(self):
        gc.disable()

    def tearDown(self):
        gc.enable()

    def test_empty_collect(self):
        cell = IOCell()
        ref = weakref.ref(cell)
        list(cell)
        del cell
        self.assertIsNone(ref())

    def test_closure_coros_collect(self):
        def wrap():
            cell = IOCell()
            class Thing(object):
                pass
            thing = Thing()
            @cell.tier()
            async def tier(r):
                await r.emit(thing)
            return cell
        c = wrap()
        ref = weakref.ref(c)
        list(c)
        del c
        self.assertIsNone(ref())

    def test_ref_to_cycles_collect(self):
        def wrap():
            cell = IOCell()
            class Thing(object):
                pass
            thing1 = Thing()
            thing1.thing2 = Thing()
            thing1.thing2.thing1 = thing1
            @cell.tier()
            async def tier1(r):
                thing1.hello = 'world'
                await r.emit(thing1)
            @cell.tier(source=tier1)
            async def tier2(r, thing):
                await r.emit(thing)
            return cell
        c = wrap()
        cellref = weakref.ref(c)
        thingref = weakref.ref(list(c)[0])
        del c
        self.assertIsNone(cellref())
        self.assertIsNotNone(thingref())

    def test_tier_collect(self):
        t = Tier(None, asyncio.coroutine(lambda: 1))
        ref = weakref.ref(t)
        del t
        self.assertIsNone(ref())


class Cleanup(unittest.TestCase):

    def test_cleaner(self):
        cell = IOCell()

        @cell.tier()
        async def coro(route):
            await route.emit(1)
        cleaned = False

        @cell.cleaner
        async def cleaner():
            nonlocal cleaned
            cleaned = True

        self.assertEqual(list(cell), [1])
        self.assertTrue(cleaned)

