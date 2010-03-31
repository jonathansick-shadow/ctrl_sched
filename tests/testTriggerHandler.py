#!/usr/bin/env python
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
