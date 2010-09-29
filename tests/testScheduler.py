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

from lsst.ctrl.sched.joboffice.scheduler import Scheduler
from lsst.ctrl.sched.joboffice.scheduler import DataTriggeredScheduler, ButlerTriggeredScheduler
from lsst.ctrl.sched import Dataset
from lsst.ctrl.sched.blackboard import Blackboard
from lsst.pex.policy import Policy, PolicyString
from lsst.daf.persistence import LogicalLocation
from lsst.daf.base import PropertySet

from lsst.pex.logging import Log
# Log.getDefaultLog().setThreshold(Log.WARN)
rootlogger = Log.getDefaultLog()
rootlogger.setThreshold(Log.WARN)


testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")
exampledir = os.path.join(os.environ["CTRL_SCHED_DIR"], "examples")
bbdir = os.path.join(testdir, "testbb")
locations = PropertySet()
locations.set("input", testdir)
LogicalLocation.setLocationMap(locations)

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
        # self.assertRaises(RuntimeError, t.makeJobsAvailable)

class DataTriggeredSchedulerTestCase(unittest.TestCase):

    def setUp(self):
        self.bb = Blackboard(bbdir)
        self.sched = None
        self.logger = Log(rootlogger, "sched")
        
    def tearDown(self):
        self.logger.setThreshold(Log.INHERIT_THRESHOLD)
        if os.path.exists(bbdir):
            os.system("rm -rf %s" % bbdir)
        self.sched = None

    def testCtor(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        sched = DataTriggeredScheduler(self.bb, spolicy, self.logger)

        self.assert_(sched.nametmpl is None)
        self.assertEquals(sched.defaultName, "Job")
        self.assertEquals(sched.nameNumber, 1)
        self.assertEquals(len(sched.triggers), 1)
        self.assertEquals(len(sched.inputdata), 1)

    def testCreateName(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        sched = DataTriggeredScheduler(self.bb, spolicy, self.logger)
        
        ds = Dataset("PostISR", ampid=3)
        self.assertEquals(sched.createName(ds), "Job-1")

    def testCreateName2(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        spolicy.set("job.name.template", "%(type)s-v%(ampid)s")
        sched = DataTriggeredScheduler(self.bb, spolicy, self.logger)
        
        ds = Dataset("PostISR", ampid=3)
        self.assertEquals(sched.createName(ds), "PostISR-v3")


    def testProcessDataset(self):
        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 0)

        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "ccdassembly-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        self.sched = DataTriggeredScheduler(self.bb, spolicy, self.logger)

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
        self.logger = Log(rootlogger, "sched")
        
    def tearDown(self):
        self.logger.setThreshold(Log.INHERIT_THRESHOLD)
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
        
        self.sched = DataTriggeredScheduler(self.bb, spolicy, self.logger)

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

        
class ButlerTriggeredSchedulerTestCase(unittest.TestCase):

    def setUp(self):
        self.bb = Blackboard(bbdir)
        self.sched = None
        self.logger = Log(rootlogger, "sched")
        
    def tearDown(self):
        if os.path.exists(bbdir):
            os.system("rm -rf %s" % bbdir)
        self.sched = None

    def testCtor(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "srcAssoc-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        sched = ButlerTriggeredScheduler(self.bb, spolicy, self.logger)

        self.assert_(sched.nametmpl is None)
        self.assertEquals(sched.defaultName, "Job")
        self.assertEquals(sched.nameNumber, 1)
        self.assertEquals(len(sched.triggers), 1)
        self.assertEquals(len(sched.inputdata), 1)

    def testCreateName(self):
        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "srcAssoc-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        sched = ButlerTriggeredScheduler(self.bb, spolicy, self.logger)
        
        ds = Dataset("PostISR", ampid=3)
        self.assertEquals(sched.createName(ds), "Job-1")

    def testProcessDataset(self):
        # self.logger.setThreshold(Log.DEBUG)
        
        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 0)

        policy = Policy.createPolicy(os.path.join(exampledir,
                                                  "srcAssoc-joboffice.paf"))
        spolicy = policy.getPolicy("schedule")
        self.sched = ButlerTriggeredScheduler(self.bb, spolicy, self.logger)

        # pdb.set_trace()
        ds = Dataset("src", visit=85408535, raft="2,2", sensor="2,2")
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 1)
            self.assertEquals(self.bb.queues.jobsPossible.length(), 1)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.getName(), "Job-1")
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 14)
            self.assertEquals(len(job.getInputDatasets()), 15)
            ods = job.getOutputDatasets()
            self.assertEquals(len(ods), 7)
            self.assertEquals(ods[0].type, "source")
            self.assert_(ods[0].ids.has_key("skyTile"))
            self.assertEquals(self.sched.nameNumber, 2)
    
        # pdb.set_trace()
        dss = [ Dataset("src", visit=85408535, raft="2,2", sensor="0,2"),
                Dataset("src", visit=85408535, raft="2,2", sensor="1,1"),
                Dataset("src", visit=85408535, raft="2,2", sensor="1,2"),
                Dataset("src", visit=85408535, raft="2,2", sensor="2,0"),
                Dataset("src", visit=85408535, raft="2,2", sensor="2,1"),
                Dataset("src", visit=85408535, raft="2,3", sensor="1,0"),
                Dataset("src", visit=85408535, raft="2,3", sensor="2,0"),
                Dataset("src", visit=85408535, raft="2,3", sensor="2,1"),
                Dataset("src", visit=85408535, raft="3,2", sensor="0,0"),
                Dataset("src", visit=85408535, raft="3,2", sensor="0,1"),
                Dataset("src", visit=85408535, raft="3,2", sensor="0,2"),
                Dataset("src", visit=85408535, raft="3,2", sensor="1,2"),
                Dataset("src", visit=85408535, raft="3,3", sensor="1,0") ]

        i = 1
        for ds in dss:
            # pdb.set_trace()
            self.sched.processDataset(ds)
            i += 1
            
            with self.bb.queues:
                self.assertEquals(self.bb.queues.dataAvailable.length(), i)
                job = self.bb.queues.jobsPossible.get(0)
                self.assertEquals(job.triggerHandler.getNeededDatasetCount(),
                                  15-i)
                
        with self.bb.queues:
            # 9 is empirical; is it really right, though?
            self.assertEquals(self.bb.queues.jobsPossible.length(), 9)

        # pdb.set_trace()
        ds = Dataset("src", visit=85408535, raft="3,3", sensor="0,0")
        self.sched.processDataset(ds)

        with self.bb.queues:
            self.assertEquals(self.bb.queues.dataAvailable.length(), 15)
            job = self.bb.queues.jobsPossible.get(0)
            self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 0)
            self.assert_(job.isReady())
            job = self.bb.queues.jobsPossible.get(1)
            self.assertEquals(job.getName(), "Job-2")
            self.assert_(not job.isReady())

        
    
    

__all__ = "AbstractSchedulerTestCase DataTriggeredSchedulerTestCase DataTriggeredSchedulerTestCase2 ButlerTriggeredSchedulerTestCase".split()

if __name__ == "__main__":
    unittest.main()
