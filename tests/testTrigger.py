#!/usr/bin/env python
"""
Tests of the Trigger classes
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.ctrl.sched.joboffice.triggers import Trigger, SimpleTrigger
from lsst.ctrl.sched.joboffice.dataset import Dataset
import lsst.ctrl.sched.joboffice.id as id
from lsst.pex.policy import Policy

class AbstractTriggerTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoCtor(self):
        self.assertRaises(RuntimeError, trig.Trigger)

    def testNoRecognizeImpl(self):
        t = Trigger(fromSubclass=True)
        self.assert_(t.recognize() is None)

class SimpleTriggerTestCase(unittest.TestCase):

    def setUp(self):
        self.type = "CalExp"
        self.ids = [ id.IntegerIDFilter("visit", values=88), 
                     id.IntegerIDFilter("ccd", 0, 9), 
                     id.IntegerIDFilter("amp", 0, 16)       ]
        self.idd = {}
        for id in self.ids:
            self.idd[id.name] = id

    def tearDown(self):
        pass

    def testDatasetType(self):
        t = SimpleTrigger(self.type)

        ds = Dataset("goob")
        self.assert_(not t.recognize(ds))

        ds = Dataset(self.type)
        self.assert_(t.recognize(ds))

    def testRecognizeIds(self):
        pass


__all__ = "AbstractTriggerTestCase SimpleTriggerTestCase".split()

if __name__ == "__main__":
    unittest.main()
