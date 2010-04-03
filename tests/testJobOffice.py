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
from lsst.pex.policy import Policy, DefaultPolicyFile

testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")
exampledir = os.path.join(os.environ["CTRL_SCHED_DIR"], "examples")
bbdir = os.path.join(testdir, "testbb")
policyFile = DefaultPolicyFile("ctrl_sched", "ccdassembly-joboffice.paf",
                               "examples")

class AbstractJobOfficeTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        if os.path.exists(bbdir):
            os.system("rm -rf %s" % bbdir)
        self.sched = None

    def testNoCtor(self):
        self.assertRaises(RuntimeError, JobOffice, bbdir)
        self.assertRaises(RuntimeError, _BaseJobOffice, bbdir)

    def testNoRunImpl(self):
        jo = JobOffice(bbdir, fromSubclass=True)
        self.assertRaises(RuntimeError, jo.run)


class DataTriggeredJobOfficeTestCase(unittest.TestCase):
    def setUp(self):
        policy = Policy.createPolicy(policyFile)
        self.joboffice = DataTriggeredJobOffice(testdir, policy=policy)
        self.jodir = os.path.join(testdir, "ccdassembly")
        
    def tearDown(self):
        self.joboffice = None
        if os.path.exists(self.jodir):
            os.system("rm -rf %s" % self.jodir)

    def testCtor(self):
        self.assert_(os.path.exists(self.jodir), "Blackboard dir not created")
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsPossible.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),0)

    def testDatasetFromProperty(self):
        data = """#<?cfg paf policy ?>
type: PostISR
ids: {
visit: 44291
ccd: 3
raft: 33
snap: 0
amp: 5
}
"""
        ds = self.joboffice.datasetFromProperty(data)
        self.assertEquals(ds.type, "PostISR")
        self.assertEquals(ds.ids["visit"], 44291)
        self.assertEquals(ds.ids["ccd"], 3)
        self.assertEquals(ds.ids["raft"], 33)
        self.assertEquals(ds.ids["snap"], 0)
        self.assertEquals(ds.ids["amp"], 5)


__all__ = "AbstractJobOfficeTestCase DataTriggeredJobOfficeTestCase".split()

if __name__ == "__main__":
    unittest.main()
