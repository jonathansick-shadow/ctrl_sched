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
Tests of the scheduler classes
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time
import copy
import random

from lsst.ctrl.sched import utils, Dataset
from lsst.ctrl.events import EventReceiver

brokerhost = "lsst8.ncsa.uiuc.edu"
postisrdata = """#<?cfg paf policy ?>
type: PostISR
valid: true
ids: {
visit: 44291
ccd: 3
raft: 33
snap: 0
ampid: 5
}
"""

class RunIdTestCase(unittest.TestCase):

    def setUp(self):
        self.base = "rlp"
        self.lim = 1000
    def tearDown(self):
        pass

    def testCreateDef(self):
        # pdb.set_trace()
        runid = utils.createRunId()
        self.assert_(runid.startswith("test"))
        self.assertEquals(len(runid), 9, "wrong length: " + runid)
        self.assertNotEquals(utils.createRunId(), runid,
                             "created duplicate runids: " + runid)

    def testCreateWBase(self):
        runid = utils.createRunId(self.base)
        self.assert_(runid.startswith(self.base))
        self.assertEquals(len(runid), len(self.base)+5,
                          "wrong length: " + runid)
        self.assertNotEquals(utils.createRunId(self.base), runid,
                             "created duplicate runids: " + runid)

    def testCreateWLim(self):
        runid = utils.createRunId(self.base, self.lim)
        self.assert_(runid.startswith(self.base))
        self.assertEquals(len(runid), len(self.base)+len(str(self.lim))-1,
                          "wrong length for lim=%d: %s" % (self.lim, runid))
        self.assertNotEquals(utils.createRunId(self.base, self.lim), runid,
                             "created duplicate runids: " + runid)

class EventSenderTestCase(unittest.TestCase):

    def setUp(self):
        self.topic = "testtopic"
        self.runid = utils.createRunId()
        self.sender = utils.EventSender(self.runid, self.topic, brokerhost)
        self.rcvr = EventReceiver(brokerhost, self.topic,
                                  "RUNID='%s'" % self.runid)

    def tearDown(self):
        self.sender = None

    def _makeDataset(self):
        return Dataset.fromPolicy(utils.unserializePolicy(postisrdata))

    def testStatusEvent(self):
        status = "channel"
        ev = self.sender.createStatusEvent(status)
        self.assertEquals(ev.getStatus(), status)
        ev.setProperty("pipelineName", "ccdAssembly")
        self.assertEquals(ev.getProperty("pipelineName"), "ccdAssembly")

        event = ev.create()
        self.assertEquals(event.getStatus(), status)
        origid = event.getOriginatorId()
        self.assert_(origid != 0)
        self.assertEquals(event.getPropertySet().getString("pipelineName"),
                          "ccdAssembly")

        ds = self._makeDataset()
        ev.addDataset("inputs", ds)
        ds.ids["ampid"] += 1
        ev.addDataset("inputs", ds)

        self.sender.send(ev.create())
        event = self.rcvr.receiveStatusEvent(1000)
        self.assert_(event is not None, "failed to receive sent event")
        self.assertEquals(event.getStatus(), status)
        self.assertEquals(event.getPropertySet().getString("pipelineName"),
                          "ccdAssembly")
        self.assertEquals(event.getOriginatorId(), origid)
        
        dslist = event.getPropertySet().getArrayString("inputs")
        if dslist is not None:
            dslist = utils.unserializeDatasetList(dslist)
        self.assert_(dslist is not None)
        self.assertEquals(len(dslist), 2)
        self.assertEquals(dslist[0].type, "PostISR")
        
    def testCommandEvent(self):
        dest = random.randint(1, 0xffff)
        self.rcvr = EventReceiver(brokerhost, self.topic,
                       "RUNID='%s' and DESTINATIONID=%d" % (self.runid, dest))

        status = "channel"
        ev = self.sender.createCommandEvent(status, dest)
        self.assertEquals(ev.getStatus(), status)
        self.assertEquals(ev.getDestinationId(), dest)
        ev.setProperty("pipelineName", "ccdAssembly")
        self.assertEquals(ev.getProperty("pipelineName"), "ccdAssembly")

        event = ev.create()
        self.assertEquals(event.getStatus(), status)
        self.assertEquals(event.getDestinationId(), dest)
        origid = event.getOriginatorId()
        self.assert_(origid != 0)
        self.assertEquals(event.getPropertySet().getString("pipelineName"),
                          "ccdAssembly")

        ds = self._makeDataset()
        ev.addDataset("inputs", ds)
        ds.ids["ampid"] += 1
        ev.addDataset("inputs", ds)

        self.sender.send(ev.create())
        event = self.rcvr.receiveCommandEvent(1000)
        self.assert_(event is not None, "failed to receive sent event")
        self.assertEquals(event.getStatus(), status)
        self.assertEquals(event.getPropertySet().getString("pipelineName"),
                          "ccdAssembly")
        self.assertEquals(event.getOriginatorId(), origid)
        self.assertEquals(event.getDestinationId(), dest)
        
        dslist = event.getPropertySet().getArrayString("inputs")
        if dslist is not None:
            dslist = utils.unserializeDatasetList(dslist)
        self.assert_(dslist is not None)
        self.assertEquals(len(dslist), 2)
        self.assertEquals(dslist[0].type, "PostISR")

    def testDatasetEvent(self):
        ds = self._makeDataset()
        ev = self.sender.createDatasetEvent("ccdAssembly", ds)
        ds.ids["ampid"] += 1
        ev.addDataset("dataset", ds)
        origid = ev.getOriginatorId()

        self.sender.send(ev)

        event = self.rcvr.receiveStatusEvent(1000)
        self.assert_(event is not None, "failed to receive sent event")
        self.assertEquals(event.getStatus(), "available")
        self.assertEquals(event.getPropertySet().getString("pipelineName"),
                          "ccdAssembly")
        self.assertEquals(event.getOriginatorId(), origid)
        
        dslist = event.getPropertySet().getArrayString("dataset")
        if dslist is not None:
            dslist = utils.unserializeDatasetList(dslist)
        self.assert_(dslist is not None)
        self.assertEquals(len(dslist), 2)
        self.assertEquals(dslist[0].type, "PostISR")
        self.assert_(dslist[0].valid)


class ImporterTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoFind(self):
        self.assertRaises(ImportError, utils.importClass,
                          "lsst.ctrl.sched.goober.Goob")
        
    def testImport(self):
        tcls = utils.importClass("lsst.ctrl.sched.joboffice.triggers.Trigger")
        self.assert_(isinstance(tcls, type))
        cls = utils.importClass("lsst.ctrl.sched.joboffice.triggers.SimpleTrigger")
        self.assert_(issubclass(cls, tcls))


__all__ = "RunIdTestCase EventSenderTestCase ImporterTestCase".split()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        brokerhost = sys.argv.pop(1)
    unittest.main()

