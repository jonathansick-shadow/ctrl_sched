#!/usr/bin/env python
"""
Tests of the scheduler classes
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.ctrl.sched.joboffice.jobOffice import JobOffice, _BaseJobOffice, DataTriggeredJobOffice
from lsst.ctrl.sched import Dataset
from lsst.pex.policy import Policy

testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")
exampledir = os.path.join(os.environ["CTRL_SCHED_DIR"], "examples")
bbdir = os.path.join(testdir, "testbb")

class AbstractJobOfficeTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoCtor(self):
        self.assertRaises(RuntimeError, JobOffice, bbdir)
        self.assertRaises(RuntimeError, _BaseJobOffice, bbdir)

    def testNoRunImpl(self):
        jo = JobOffice(bbdir, fromSubclass=True)
        self.assertRaises(RuntimeError, jo.run)


class DataTriggeredJobOfficeTestCase(unittest.TestCase):
    pass


__all__ = "AbstractJobOfficeTestCase".split()

if __name__ == "__main__":
    unittest.main()
