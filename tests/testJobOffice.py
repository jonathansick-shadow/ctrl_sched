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

from lsst.ctrl.sched.joboffice.jobOffice import JobOffice, _BaseJobOffice, DataTriggeredJobOffice, unserializePolicy
from lsst.ctrl.sched.blackboard.item import JobItem, DataProductItem
from lsst.ctrl.sched import Dataset
from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.daf.base import PropertySet
from lsst.ctrl.events import StatusEvent, CommandEvent

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
amp: 5
}
"""

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
        ds = self.joboffice.datasetFromProperty(postisrdata)
        self.assertEquals(ds.type, "PostISR")
        self.assertEquals(ds.ids["visit"], 44291)
        self.assertEquals(ds.ids["ccd"], 3)
        self.assertEquals(ds.ids["raft"], 33)
        self.assertEquals(ds.ids["snap"], 0)
        self.assertEquals(ds.ids["amp"], 5)
        return ds

    def testToPipelineQueueItem(self):
        pipelineName = "ccdassembly"
        ps = PropertySet()
        ps.set("pipelineName", pipelineName)
        ps.set("STATUS", "done")
        pevent = StatusEvent("testing", ps)
        
        item = self.joboffice.toPipelineQueueItem(pevent)
        self.assertEquals(item.getName(), pipelineName)
        self.assertEquals(item.getProperty("status"), "done")
        self.assertEquals(item.getProperty("runid"), "testing")

    def testMakeJobCommandEvent(self):
        ds = self.testDatasetFromProperty()
        dss = [ds]
        for i in xrange(5):
            ds = copy.deepcopy(ds)
            ds.ids["amp"] += 1
            dss.append(ds)

        job = JobItem.createItem("ccdassembly", dss)
        jev = self.joboffice.makeJobCommandEvent(job, 9993252, "testing")

        self.assertEquals(jev.getStatus(), "process")
        self.assertEquals(jev.getRunId(), "testing")
        self.assertEquals(jev.getDestinationId(), 9993252)
        self.assert_(jev.getPropertySet().exists("dataset"))

        dodss = jev.getPropertySet().getArrayString("dataset")
        self.assertEquals(len(dodss), 6)
        i = 5
        for ds in dodss:
            ds = Dataset.fromPolicy(unserializePolicy(ds))
            self.assertEquals(ds.type, "PostISR")
            self.assertEquals(ds.ids["amp"], i)
            self.assertEquals(ds.ids["visit"], 44291)
            self.assertEquals(ds.ids["ccd"], 3)
            self.assertEquals(ds.ids["raft"], 33)
            self.assertEquals(ds.ids["snap"], 0)
            i += 1

    def testProcessDataEvent(self):
        ds = self.testDatasetFromProperty()
        dss = [ds]
        for i in xrange(3):
            ds = copy.deepcopy(ds)
            ds.ids["amp"] += 1
            dss.append(ds)

        ps = PropertySet()
        ps.set("pipelineName", "PostISR")
        ps.set("STATUS", "available")
        pevent = StatusEvent("testing", ps)
        


__all__ = "AbstractJobOfficeTestCase DataTriggeredJobOfficeTestCase".split()

if __name__ == "__main__":
    unittest.main()
