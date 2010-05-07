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

from lsst.ctrl.sched.joboffice.scheduler import Scheduler, DataTriggeredScheduler
from lsst.ctrl.sched import Dataset
from lsst.ctrl.sched.blackboard import Blackboard
from lsst.pex.policy import Policy, PolicyString

testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")
exampledir = os.path.join(os.environ["CTRL_SCHED_DIR"], "examples")
bbdir = os.path.join(testdir, "testbb")

class AbstractSchedulerTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoCtor(self):
        self.assertRaises(RuntimeError, Scheduler, None)

    def testNoRecognizeImpl(self):
        t = Scheduler(None, fromSubclass=True)
        self.assertRaises(RuntimeError, t.processDataset, None, True)
        self.assertRaises(RuntimeError, t.makeJobsAvailable)

class DataTriggeredSchedulerTestCase(unittest.TestCase):

    def setUp(self):
        self.bb = Blackboard(bbdir)
        self.sched = None
        
    def tearDown(self):
        if os.path.exists(bbdir):
            os.system("rm -rf %s" % bbdir)
        self.sched = None

    def testCtor(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        sched = DataTriggeredScheduler(self.bb, spolicy)

        self.assert_(sched.nametmpl is None)
        self.assertEquals(sched.defaultName, "Job")
        self.assertEquals(sched.nameNumber, 1)
        self.assertEquals(len(sched.triggers), 1)
        self.assertEquals(len(sched.inputdata), 1)

    def testCreateName(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        sched = DataTriggeredScheduler(self.bb, spolicy)
        
        ds = Dataset("PostISR", ampid=3)
        self.assertEquals(sched.createName(ds), "Job-1")

    def testCreateName2(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        spolicy.set("job.name.template", "%(type)s-v%(ampid)s")
        sched = DataTriggeredScheduler(self.bb, spolicy)
        
        ds = Dataset("PostISR", ampid=3)
        self.assertEquals(sched.createName(ds), "PostISR-v3")


    def testProcessDataset(self):
        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 0)

        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        self.sched = DataTriggeredScheduler(self.bb, spolicy)

        # pdb.set_trace()
        ds = Dataset("PostISR", visitid=88, ccdid=22, snapid=0, ampid=15)
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 1)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 1)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.getName(), "Job-1")
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 15)
            self.assertEquals(self.sched.nameNumber, 2)
    
        ds = Dataset("PostISR", visitid=95, ccdid=22, snapid=0, ampid=15)
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 2)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 2)
            job = self.bb.queues.jobsPossible.get(1)
            self.assertEquals(job.getName(), "Job-2")
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 15)
            inputs = job.getInputDatasets()
            self.assertEquals(len(inputs), 16)
            self.assertEquals(inputs[0].type, "PostISR")
            self.assertEquals(self.sched.nameNumber, 3)

        ds = Dataset("PostISR", visitid=88, ccdid=22, snapid=0, ampid=14)
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 3)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 2)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 14)

        # pdb.set_trace()
        for i in xrange(14):
            ds = Dataset("PostISR", visitid=88, ccdid=22, snapid=0, ampid=i)
            self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 17)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 2)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 0)
            self.assert_(job.isReady())

    def testMakeAvail(self):
        self.testProcessDataset()

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 17)
            self.assertEquals(self.bb.queues.jobsAvailable.length(), 0)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 2)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.getName(), "Job-1")
            self.assert_(job.isReady())
            job = self.bb.queues.jobsPossible.get(1)
            self.assertEquals(job.getName(), "Job-2")
            self.assert_(not job.isReady())

        self.sched.makeJobsAvailable()

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 17)
            self.assertEquals(self.bb.queues.jobsAvailable.length(), 1)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 1)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.getName(), "Job-2")
            self.assert_(not job.isReady())
            job = self.bb.queues.jobsAvailable.get(0)
            self.assertEquals(job.getName(), "Job-1")

idpolicy = """#<?cfg paf policy ?>
  datasetType:  PostISR-CCD
  id: visitid
  id: ccdid
  id: snapid
"""
        
class DataTriggeredSchedulerTestCase2(unittest.TestCase):
    """
    test for a specific (unticketed) coding bug in _determineJobIdentity().
    """

    def setUp(self):
        self.bb = Blackboard(bbdir)
        self.sched = None
        
    def tearDown(self):
        if os.path.exists(bbdir):
            os.system("rm -rf %s" % bbdir)
        self.sched = None

    def testProcessDataset(self):
        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 0)

        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")

        # manipulate the policy
        idp = Policy.createPolicy(PolicyString(idpolicy))
        spolicy.set("job.identity", idp)
        
        self.sched = DataTriggeredScheduler(self.bb, spolicy)

        # pdb.set_trace()
        ds = Dataset("PostISR", visitid=88, ccdid=22, snapid=0, ampid=15)
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 1)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 1)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.getName(), "Job-1")
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 15)
            self.assertEquals(self.sched.nameNumber, 2)
    
        ds = Dataset("PostISR", visitid=95, ccdid=22, snapid=0, ampid=15)
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 2)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 2)
            job = self.bb.queues.jobsPossible.get(1)
            self.assertEquals(job.getName(), "Job-2")
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 15)
            inputs = job.getInputDatasets()
            self.assertEquals(len(inputs), 16)
            self.assertEquals(inputs[0].type, "PostISR")
            self.assertEquals(self.sched.nameNumber, 3)

        ds = Dataset("PostISR", visitid=88, ccdid=22, snapid=0, ampid=14)
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 3)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 2)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 14)

        # pdb.set_trace()
        for i in xrange(14):
            ds = Dataset("PostISR", visitid=88, ccdid=22, snapid=0, ampid=i)
            self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 17)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 2)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 0)
            self.assert_(job.isReady())

        

    

__all__ = "AbstractSchedulerTestCase DataTriggeredSchedulerTestCase DataTriggeredSchedulerTestCase2".split()

if __name__ == "__main__":
    unittest.main()
