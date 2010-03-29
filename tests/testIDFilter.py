#!/usr/bin/env python
"""
Tests of the IDFilter classes
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

import lsst.ctrl.sched.joboffice.id as id
from lsst.pex.policy import Policy

class AbstractIDFilterTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoCtor(self):
        self.assertRaises(RuntimeError, id.IDFilter, "Goofy")

    def testNoRecognizeImpl(self):
        idf = id.IDFilter("Goofy", fromSubclass=True)
        self.assertRaises(RuntimeError, idf.recognize, 1)

class IntegerIDFilterTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoConstraints(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit")
        self.assertEquals(idf.name, "visit")
        self.assertEquals(idf.outname, "visit")

        self.assert_(idf.recognize(2) is None)
        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assert_(idf.recognize(3) is None)
        

    def testMin(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", 3)
        self.assertEquals(idf.name, "visit")

        self.assert_(idf.recognize(2) is None)
        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assertEquals(idf.recognize("50"), 50)
        self.assertEquals(idf.recognize(3), 3)

    def testLim(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", lim=3)
        self.assertEquals(idf.name, "visit")

        self.assertEquals(idf.recognize(2), 2)
        self.assertEquals(idf.recognize(-1), -1)
        self.assertEquals(idf.recognize("-1"), -1)
        self.assert_(idf.recognize("50") is None)
        self.assert_(idf.recognize("50") is None)

    def testRange(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", 0, 16)
        self.assertEquals(idf.name, "visit")

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assertEquals(idf.recognize(0), 0)
        self.assertEquals(idf.recognize(15), 15)
        self.assert_(idf.recognize(16) is None)

    def testValues1(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", values=range(16))
        self.assertEquals(idf.name, "visit")

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assertEquals(idf.recognize(0), 0)
        self.assertEquals(idf.recognize(15), 15)
        self.assert_(idf.recognize(16) is None)

    def testValues2(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", values=[3, 6, -8])
        self.assertEquals(idf.name, "visit")

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assert_(idf.recognize(0) is None)
        self.assert_(idf.recognize(15) is None)
        self.assertEquals(idf.recognize(6), 6)
        self.assertEquals(idf.recognize(-8), -8)
        self.assert_(idf.recognize(16) is None)

    def testValues3(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", values=3)
        self.assertEquals(idf.name, "visit")

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assert_(idf.recognize(0) is None)
        self.assert_(idf.recognize(15) is None)

    def testValues4(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", 0, 16, values=[20,25])
        self.assertEquals(idf.name, "visit")

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assertEquals(idf.recognize(0), 0)
        self.assertEquals(idf.recognize(15), 15)
        self.assert_(idf.recognize(16) is None)
        self.assertEquals(idf.recognize(20), 20)
        self.assert_(idf.recognize(23) is None)
        self.assertEquals(idf.recognize(25), 25)

    def testBadValues(self):
        self.assertRaises(ValueError, id.IntegerIDFilter, "visit", values="4 9 7".split())
        self.assertRaises(ValueError, id.IntegerIDFilter, "visit", values=[3, "6", -8])
        self.assertRaises(ValueError, id.IntegerIDFilter, "visit", values="6")

    def testFromPolicy(self):
        p = Policy()
        p.set("name", "visit")
        idf = id.IntegerIDFilter.fromPolicy(p)
        self.testNoConstraints(idf)

        idf = id.IDFilter.fromPolicy(p)
        self.testNoConstraints(idf)
        p.set("className", "Integer")
        idf = id.IDFilter.fromPolicy(p)
        self.testNoConstraints(idf)
        p.set("className", "IntegerIDFilter")
        idf = id.IDFilter.fromPolicy(p)
        self.testNoConstraints(idf)

        p.set("className", "lsst.ctrl.sched.joboffice.id.IntegerIDFilter")
        self.assertRaises(RuntimeError, id.IDFilter.fromPolicy, p)
        p.set("className", "Integer")
        
        p.set("min", 3)
        idf = id.IDFilter.fromPolicy(p)
        self.testMin(idf)
        p.set("min", 0)
        p.set("lim", 16)
        idf = id.IDFilter.fromPolicy(p)
        self.testRange(idf)
        p.set("values", 20)
        p.add("values", 25)
        idf = id.IDFilter.fromPolicy(p)
        self.testValues4(idf)


__all__ = "AbstractIDFilterTestCase IntegerIDFilterTestCase".split()

if __name__ == "__main__":
    unittest.main()
