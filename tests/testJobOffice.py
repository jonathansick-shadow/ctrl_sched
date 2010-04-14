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
import copy

from lsst.ctrl.sched.joboffice.jobOffice import JobOffice, _BaseJobOffice, DataTriggeredJobOffice, unserializePolicy, serializePolicy
from lsst.ctrl.sched.blackboard.item import JobItem, DataProductItem
from lsst.ctrl.sched import Dataset
from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.daf.base import PropertySet
from lsst.ctrl.events import StatusEvent, CommandEvent, EventTransmitter, EventSystem

testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")
exampledir = os.path.join(os.environ["CTRL_SCHED_DIR"], "examples")
bbdir = os.path.join(testdir, "testbb")
policyFile = DefaultPolicyFile("ctrl_sched", "ccdassembly-joboffice.paf",
                               "examples")
postisrdata = """#<?cfg paf policy ?>
type: PostISR
ids: {
visit: 44291
ccd: 3
raft: 33
snap: 0
ampid: 5
}
"""
brokerhost = "lsst8.ncsa.uiuc.edu"
originatorId = EventSystem.getDefaultEventSystem().createOriginatorId()

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
        self.assertRaises(RuntimeError, jo.managePipelines)


class DataTriggeredJobOfficeTestCase(unittest.TestCase):
    def setUp(self):
        policy = Policy.createPolicy(policyFile)
        # pdb.set_trace()
        self.joboffice = DataTriggeredJobOffice(testdir, policy=policy,
                                                brokerHost=brokerhost)
        self.joboffice.log.setThreshold(self.joboffice.log.WARN)
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
        ds = self.joboffice.datasetFromProperty(postisrdata)
        self.assertEquals(ds.type, "PostISR")
        self.assertEquals(ds.ids["visit"], 44291)
        self.assertEquals(ds.ids["ccd"], 3)
        self.assertEquals(ds.ids["raft"], 33)
        self.assertEquals(ds.ids["snap"], 0)
        self.assertEquals(ds.ids["ampid"], 5)
        return ds

    def testToPipelineQueueItem(self):
        pipelineName = "ccdassembly"
        ps = PropertySet()
        ps.set("pipelineName", pipelineName)
        ps.set("STATUS", "done")
        ps.set("RUNID", "testing")
        pevent = StatusEvent("testing", originatorId, ps)

        item = self.joboffice.toPipelineQueueItem(pevent)
        self.assertEquals(item.getName(), pipelineName)
        self.assertEquals(item.getProperty("status"), "done")
        self.assertEquals(item.getRunId(), "testing")

    def testMakeJobCommandEvent(self):
        ds = self.testDatasetFromProperty()
        dss = [ds]
        for i in xrange(1, 5):
            ds = copy.deepcopy(ds)
            ds.ids["ampid"] += 1
            dss.append(ds)
        ods = Dataset("PostISR-CCD", visit=ds.ids["visit"], ccd=ds.ids["ccd"])

        job = JobItem.createItem(ods, "ccdassembly", dss, ods)
        jev = self.joboffice.makeJobCommandEvent(job, 9993252, "testing")

        self.assertEquals(jev.getStatus(), "job:assign")
        self.assertEquals(jev.getRunId(), "testing")
        self.assertEquals(jev.getDestinationId(), 9993252)
        self.assert_(jev.getPropertySet().exists("inputs"))
        self.assert_(jev.getPropertySet().exists("outputs"))

        dodss = jev.getPropertySet().getArrayString("inputs")
        self.assertEquals(len(dodss), 5)
        i = 5
        for ds in dodss:
            ds = Dataset.fromPolicy(unserializePolicy(ds))
            self.assertEquals(ds.type, "PostISR")
            self.assertEquals(ds.ids["ampid"], i)
            self.assertEquals(ds.ids["visit"], 44291)
            self.assertEquals(ds.ids["ccd"], 3)
            self.assertEquals(ds.ids["raft"], 33)
            self.assertEquals(ds.ids["snap"], 0)
            i += 1

    def testProcessDataEvent(self):
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(), 0)

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "available")

        ds = self.testDatasetFromProperty()
        ps.add("dataset", serializePolicy(ds.toPolicy()))
        for i in xrange(1,4):
            ds = copy.deepcopy(ds)
            ds.ids["ampid"] += 1
            ps.add("dataset", serializePolicy(ds.toPolicy()))

        devent = StatusEvent("testing", originatorId, ps)

        # pdb.set_trace()
        self.joboffice.processDataEvent(devent)

        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(), 4)
          self.assertEquals(self.joboffice.bb.queues.jobsPossible.length(), 1)
          job = self.joboffice.bb.queues.jobsPossible.get(0)
          self.assertEquals(job.getName(), "Job-1")
          self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 12)
    
    def testProcessDataEvents(self):
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(), 0)

        trx = EventTransmitter(brokerhost, "PostISRAvailable")

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "available")

        ds = self.testDatasetFromProperty()
        ds.ids["ampid"] = 0;
        ps.set("dataset", serializePolicy(ds.toPolicy()))
        # pdb.set_trace()
        for i in xrange(15):
            ps.set("dataset", serializePolicy(ds.toPolicy()))
            devent = StatusEvent("testing", originatorId, ps)

            trx.publishEvent(devent);

            ds = copy.deepcopy(ds)
            ds.ids["ampid"] += 1

        self.joboffice.processDataEvents()
        
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),15)
          self.assertEquals(self.joboffice.bb.queues.jobsPossible.length(), 1)
          job = self.joboffice.bb.queues.jobsPossible.get(0)
          self.assertEquals(job.getName(), "Job-1")
          self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 1)

        ps.set("dataset", serializePolicy(ds.toPolicy()))
        devent = StatusEvent("testing", originatorId, ps)
        trx.publishEvent(devent);
        self.joboffice.processDataEvents()
        
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),16)
          self.assertEquals(self.joboffice.bb.queues.jobsPossible.length(), 1)
          job = self.joboffice.bb.queues.jobsPossible.get(0)
          self.assertEquals(job.getName(), "Job-1")
          self.assertEquals(job.triggerHandler.getNeededDatasetCount(), 0)

    def testFindAvailableJobs(self):
        self.testProcessDataEvents()

        self.joboffice.findAvailableJobs()
        
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),16)
          self.assertEquals(self.joboffice.bb.queues.jobsPossible.length(), 0)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(), 1)
          job = self.joboffice.bb.queues.jobsAvailable.get(0)
          self.assertEquals(job.getName(), "Job-1")

    def testReceiveReadyPipelines(self):
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.pipelinesReady.length(),0)

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "job:ready")
        pevent = StatusEvent("testing", originatorId, ps)
        
        trx = EventTransmitter(brokerhost, "CcdAssemblyJob")
        trx.publishEvent(pevent)

        self.joboffice.receiveReadyPipelines()

        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.pipelinesReady.length(),1)
        
    def testAllocateJobs(self):
        # pdb.set_trace()
        self.testReceiveReadyPipelines()
        self.testFindAvailableJobs()

        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.pipelinesReady.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)

        # pdb.set_trace()
        self.joboffice.allocateJobs()

        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.pipelinesReady.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),1)

    def testProcessJobDoneEvent(self):
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "job:done")
        ps.set("success", True)
        pevent = StatusEvent("testing", originatorId, ps)

        self.assert_(not self.joboffice.processJobDoneEvent(pevent))

        self.testAllocateJobs()
        
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)

        # pdb.set_trace()
        self.assert_(self.joboffice.processJobDoneEvent(pevent))
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),1)
        
    def testProcessJobDoneEvents(self):
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)

        self.testAllocateJobs()
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "job:done")
        ps.set("success", True)
        pevent = StatusEvent("testing", originatorId, ps)
        trx = EventTransmitter(brokerhost, "CcdAssemblyJob")
        trx.publishEvent(pevent)

        self.joboffice.processDoneJobs()
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),1)


    def testRun(self):
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),0)

        trxpipe = EventTransmitter(brokerhost, "CcdAssemblyJob")
        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "job:ready")
        pevent = StatusEvent("testing", originatorId, ps)
        trxpipe.publishEvent(pevent)

        self.joboffice.managePipelines(1)
        
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.pipelinesReady.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)

        trxdata = EventTransmitter(brokerhost, "PostISRAvailable")

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "available")

        ds = self.testDatasetFromProperty()
        ds.ids["ampid"] = 0;
        ps.set("dataset", serializePolicy(ds.toPolicy()))
        # pdb.set_trace()
        for i in xrange(16):
            ps.set("dataset", serializePolicy(ds.toPolicy()))
            devent = StatusEvent("testing", originatorId, ps)

            trxdata.publishEvent(devent);

            ds = copy.deepcopy(ds)
            ds.ids["ampid"] += 1

        self.joboffice.managePipelines(1)
        
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),16)

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "job:done")
        ps.set("success", True)
        jevent = StatusEvent("testing", originatorId, ps)
        trxpipe.publishEvent(jevent)

        self.joboffice.managePipelines(1)
        
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),16)

    def testRunInThread(self):
      self.assert_(not self.joboffice.isAlive())
      self.joboffice.start()
      self.assert_(self.joboffice.isAlive())

      try:
        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),0)

        trxpipe = EventTransmitter(brokerhost, "CcdAssemblyJob")
        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "job:ready")
        pevent = StatusEvent("testing", originatorId, ps)
        trxpipe.publishEvent(pevent)
        time.sleep(1.0)

        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.pipelinesReady.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)

        trxdata = EventTransmitter(brokerhost, "PostISRAvailable")

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "available")

        ds = self.testDatasetFromProperty()
        ds.ids["ampid"] = 0;
        ps.set("dataset", serializePolicy(ds.toPolicy()))
        # pdb.set_trace()
        for i in xrange(16):
            ps.set("dataset", serializePolicy(ds.toPolicy()))
            devent = StatusEvent("testing", originatorId, ps)

            trxdata.publishEvent(devent);

            ds = copy.deepcopy(ds)
            ds.ids["ampid"] += 1
        time.sleep(1.0)

        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),16)

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "job:done")
        ps.set("success", True)
        jevent = StatusEvent("testing", originatorId, ps)
        trxpipe.publishEvent(jevent)
        time.sleep(1.0)

        with self.joboffice.bb.queues:
          self.assertEquals(self.joboffice.bb.queues.jobsInProgress.length(),0)
          self.assertEquals(self.joboffice.bb.queues.jobsDone.length(),1)
          self.assertEquals(self.joboffice.bb.queues.jobsAvailable.length(),0)
          self.assertEquals(self.joboffice.bb.queues.dataAvailable.length(),16)

      finally:
        self.joboffice.stop()
        if self.joboffice.isAlive():
            self.joboffice.join(10.0)

            

        
        
        
            


__all__ = "AbstractJobOfficeTestCase DataTriggeredJobOfficeTestCase".split()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        brokerhost = sys.argv.pop(1)
    unittest.main()
