#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

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
        self.assert_(idf.isUnconstrained())
        self.assertEquals(idf.name, "visit")
        self.assertEquals(idf.outname, "visit")

        self.assertEquals(idf.recognize(2), 2)
        self.assertEquals(idf.recognize(-1), -1)
        self.assertEquals(idf.recognize("-1"), -1)
        self.assert_(idf.recognize("5,0") is None)
        self.assertEquals(idf.recognize(3), 3)
        

    def testMin(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", 3)
        self.assertEquals(idf.name, "visit")
        self.assert_(not idf.isUnconstrained())

        self.assert_(idf.recognize(2) is None)
        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assertEquals(idf.recognize("50"), 50)
        self.assertEquals(idf.recognize(3), 3)

    def testLim(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", lim=3)
        self.assertEquals(idf.name, "visit")
        self.assert_(not idf.isUnconstrained())

        self.assertEquals(idf.recognize(2), 2)
        self.assertEquals(idf.recognize(-1), -1)
        self.assertEquals(idf.recognize("-1"), -1)
        self.assert_(idf.recognize("50") is None)
        self.assert_(idf.recognize("50") is None)

    def testRange(self, idf=None):
        if not idf:
            idf = id.IntegerIDFilter("visit", 0, 16)
        self.assertEquals(idf.name, "visit")
        self.assert_(not idf.isUnconstrained())

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
        self.assert_(not idf.isUnconstrained())

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
        self.assert_(not idf.isUnconstrained())

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
        self.assert_(not idf.isUnconstrained())

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
        self.assert_(not idf.isUnconstrained())

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

    def testAllowed(self):
        idf = id.IntegerIDFilter("visit", 0, 16, values=[20,25])
        self.assert_(idf.hasStaticValueSet())

        vals = idf.allowedValues()
        self.assertEquals(len(vals), 18)
        self.assertEquals(vals[0], 0)
        self.assertEquals(vals[-3], 15)
        self.assertEquals(vals[-2], 20)
        self.assertEquals(vals[-1], 25)

        idf = id.IntegerIDFilter("visit", 0)
        self.assert_(not idf.hasStaticValueSet())
        self.assertRaises(RuntimeError, idf.allowedValues)

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
        self.assert_(not isinstance(idf, id.IntegerIDFilter))
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
        p.set("value", 20)
        p.add("value", 25)
        idf = id.IDFilter.fromPolicy(p)
        self.testValues4(idf)

class StringIDFilterTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoConstraints(self, idf=None):
        if not idf:
            idf = id.StringIDFilter("visit")
        self.assert_(idf.isUnconstrained())
        self.assertEquals(idf.name, "visit")
        self.assertEquals(idf.outname, "visit")

        self.assert_(idf.recognize(2), "2")
        self.assert_(idf.recognize(-1), "-1")
        self.assert_(idf.recognize("-1"), "-1")
        self.assert_(idf.recognize("50"), "50")
        self.assert_(idf.recognize(3), "3")
        
    def testValues1(self, idf=None):
        if not idf:
            idf = id.StringIDFilter("visit", values="3 0 15 14 4 5".split())
        self.assertEquals(idf.name, "visit")

        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize(3), "3")
        self.assertEquals(idf.recognize("0"), "0")
        self.assertEquals(idf.recognize("15"), "15")
        self.assert_(idf.recognize("16") is None)

    def testValues2(self, idf=None):
        if not idf:
            idf = id.StringIDFilter("visit", "r 6,0 -8".split())
        self.assertEquals(idf.name, "visit")

        self.assert_(not idf.isUnconstrained())
        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("-1") is None)
        self.assert_(idf.recognize("50") is None)
        self.assertEquals(idf.recognize("r"), "r")
        self.assert_(idf.recognize("0") is None)
        self.assert_(idf.recognize("zub") is None)
        self.assertEquals(idf.recognize("6,0"), "6,0")
        self.assertEquals(idf.recognize("-8"), "-8")
        self.assert_(idf.recognize("16") is None)

    def testValues3(self, idf=None):
        if not idf:
            idf = id.StringIDFilter("visit", values="r")
        self.assertEquals(idf.name, "visit")

        self.assert_(not idf.isUnconstrained())
        self.assert_(idf.recognize(-1) is None)
        self.assert_(idf.recognize("") is None)
        self.assert_(idf.recognize("b") is None)
        self.assertEquals(idf.recognize("r"), "r")
        self.assert_(idf.recognize(0) is None)
        self.assert_(idf.recognize(15) is None)

    def testAllowed(self):
        idf = id.StringIDFilter("visit", "the quick brown".split())
        self.assert_(idf.hasStaticValueSet())
        self.assert_(not idf.isUnconstrained())

        vals = idf.allowedValues()
        self.assertEquals(len(vals), 3)
        self.assertEquals(vals[0], "brown")
        self.assertEquals(vals[1], "quick")
        self.assertEquals(vals[2], "the")

    def testBadValues(self):
        self.assertRaises(ValueError, id.StringIDFilter, "visit", values=range(4))
        self.assertRaises(ValueError, id.StringIDFilter, "visit", values=[3, "6", -8])
        self.assertRaises(ValueError, id.StringIDFilter, "visit", values=6)

    def testFromPolicy(self):
        p = Policy()
        p.set("name", "visit")
        idf = id.IntegerIDFilter.fromPolicy(p)
        self.testNoConstraints(idf)

        idf = id.IDFilter.fromPolicy(p)
        self.assert_(isinstance(idf, id.StringIDFilter))
        p.set("className", "String")
        idf = id.IDFilter.fromPolicy(p)
        self.assert_(isinstance(idf, id.StringIDFilter))
        self.testNoConstraints(idf)
        p.set("className", "StringIDFilter")
        idf = id.IDFilter.fromPolicy(p)
        self.assert_(isinstance(idf, id.StringIDFilter))
        self.testNoConstraints(idf)

        p.set("className", "lsst.ctrl.sched.joboffice.id.StringIDFilter")
        self.assertRaises(RuntimeError, id.IDFilter.fromPolicy, p)
        p.set("className", "String")
        
        p.set("value", "-8")
        p.add("value", "r")
        p.add("value", "6,0")
        idf = id.IDFilter.fromPolicy(p)
        self.assert_(isinstance(idf, id.StringIDFilter))
        self.testValues2(idf)




__all__ = "AbstractIDFilterTestCase IntegerIDFilterTestCase StringIDFilterTestCase".split()

if __name__ == "__main__":
    unittest.main()
