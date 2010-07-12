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
Tests of the TriggerHandler classes
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.ctrl.sched.joboffice.triggerHandlers import *

class AbstractTriggerHandlerTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoCtor(self):
        self.assertRaises(RuntimeError, TriggerHandler)

    def testNoRecognizeImpl(self):
        t = TriggerHandler(fromSubclass=True)
        self.assert_(not t.isReady())
        self.assertRaises(RuntimeError, t.addDataset, None)

class FilesetTriggerHandlerTestCase(unittest.TestCase):

    def setUp(self):
        self.dslist = [ Dataset("SrcList", visit=32),
                        Dataset("SrcList", visit=33),
                        Dataset("SrcList", visit=35) ]
    def tearDown(self):
        pass

    def testCtor(self):
        th = FilesetTriggerHandler()
        self.assertEquals(th.getNeededDatasetCount(), 0)
        self.assert_(th.isReady())

        th = FilesetTriggerHandler(self.dslist[0])
        self.assertEquals(th.getNeededDatasetCount(), 1)
        self.assert_(not th.isReady())
        self.assert_(str(self.dslist[0]) in th.dids)

        th = FilesetTriggerHandler(self.dslist)
        self.assertEquals(th.getNeededDatasetCount(), 3)
        self.assert_(not th.isReady())
        self.assert_(str(self.dslist[0]) in th.dids)
        self.assert_(str(self.dslist[1]) in th.dids)
        self.assert_(str(self.dslist[2]) in th.dids)

    def testAdd(self):
        th = FilesetTriggerHandler(self.dslist)
        self.assertEquals(th.getNeededDatasetCount(), 3)
        self.assert_(not th.isReady())

        th.addDataset(self.dslist[1])
        self.assertEquals(th.getNeededDatasetCount(), 2)
        self.assert_(not th.isReady())

        th.addDataset(self.dslist[0])
        self.assertEquals(th.getNeededDatasetCount(), 1)
        self.assert_(not th.isReady())

        th.addDataset(self.dslist[2])
        self.assertEquals(th.getNeededDatasetCount(), 0)
        self.assert_(th.isReady())


__all__ = "AbstractTriggerHandlerTestCase FilesetTriggerHandlerTestCase".split()

if __name__ == "__main__":
    unittest.main()
