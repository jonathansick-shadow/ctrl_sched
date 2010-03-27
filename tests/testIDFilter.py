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

    def testNoConstraints(self):
        idf = IntegerIDFilter("CalExp")
        self.assertEquals(idf.name, "CalExp")
        self.assert_(idf.outname is None)

        self.assert_(idf.recognize(2) is None)
        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assert_(idf.recognize(3) is None)
        

    def testMin(self):
        idf = IntegerIDFilter("CalExp", 3)
        self.assertEquals(idf.name, "CalExp")
        self.assert_(idf.outname is None)

        self.assert_(idf.recognize(2) is None)
        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assertEquals(idf.recognize("50"), 50)
        self.assertEquals(idf.recognize(3), 3)

    def testLim(self):
        idf = IntegerIDFilter("CalExp", lim=3)
        self.assertEquals(idf.name, "CalExp")
        self.assert_(idf.outname is None)

        self.assertEquals(idf.recognize(2), 2)
        self.assertEquals(idf.recognize(-1), -1)
        self.assertEquals(idf.recognize("-1"), -1)
        self.assert_(idf.recognize("50") is None)
        self.assert_(idf.recognize("50") is None)

    def testRange(self):
        idf = IntegerIDFilter("CalExp", 0, 16)
        self.assertEquals(idf.name, "CalExp")
        self.assert_(idf.outname is None)

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assertEquals(idf.recognize(0), 0)
        self.assertEquals(idf.recognize(15), 15)
        self.assert_(idf.recognize(16) is None)

    def testValues1(self):
        idf = IntegerIDFilter("CalExp", values=range(16))
        self.assertEquals(idf.name, "CalExp")
        self.assert_(idf.outname is None)

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assertEquals(idf.recognize(0), 0)
        self.assertEquals(idf.recognize(15), 15)
        self.assert_(idf.recognize(16) is None)

    def testValues2(self):
        idf = IntegerIDFilter("CalExp", values=[3, 6, -8])
        self.assertEquals(idf.name, "CalExp")
        self.assert_(idf.outname is None)

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assert_(idf.recognize(0) is None)
        self.assert_(idf.recognize(15) is None)
        self.assertEquals(idf.recognize(6), 6)
        self.assertEquals(idf.recognize(-8), -8)
        self.assert_(idf.recognize(16) is None)

    def testValues3(self):
        idf = IntegerIDFilter("CalExp", values=3)
        self.assertEquals(idf.name, "CalExp")
        self.assert_(idf.outname is None)

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), 3)
        self.assert_(idf.recognize(0) is None)
        self.assert_(idf.recognize(15) is None)

    def testBadValues(self):
        self.assertRaises(ValueError, IntegerIDFilter, "CalExp", values="4 9 7".split())
        self.assertRaises(ValueError, IntegerIDFilter, "CalExp", values=[3, "6", -8])
        self.assertRaises(ValueError, IntegerIDFilter, "CalExp", values="6")

