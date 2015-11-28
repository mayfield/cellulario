"""
Garbage collection verification.

A design goal of cellulario is to avoid circular references so that cleanup
of cells is immediate.  This is partially a best practice and secondly good
for setting off the assertions in asyncio code that fire when their objects
are deleted.
"""


import gc
import unittest
import weakref
from cellulario import IOCell


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
            @cell.tier_coroutine()
            def tier(t):
                yield from t.emit(thing)
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
            @cell.tier_coroutine()
            def tier1(t):
                thing1.hello = 'world'
                yield from t.emit(thing1)
            @cell.tier_coroutine(source=tier1)
            def tier2(t, thing):
                yield from t.emit(thing)
            return cell
        c = wrap()
        cellref = weakref.ref(c)
        thingref = weakref.ref(list(c)[0])
        del c
        self.assertIsNone(cellref())
        self.assertIsNotNone(thingref())
