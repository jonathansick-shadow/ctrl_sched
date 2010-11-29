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
Tests of the Trigger classes
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time
import copy

from lsst.ctrl.sched.joboffice.triggers import Trigger, SimpleTrigger, MapperTrigger
from lsst.ctrl.sched import Dataset
from lsst.ctrl.sched.joboffice.id import IDFilter, IntegerIDFilter
from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.daf.persistence import LogicalLocation
from lsst.daf.base import PropertySet

from lsst.pex.logging import Log
Log.getDefaultLog().setThreshold(Log.WARN)

testdir = os.path.join(os.environ['CTRL_SCHED_DIR'], 'tests')
locations = PropertySet()
locations.set("input", testdir)
LogicalLocation.setLocationMap(locations)

class AbstractTriggerTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testNoCtor(self):
        self.assertRaises(RuntimeError, Trigger)

    def testNoRecognizeImpl(self):
        t = Trigger(fromSubclass=True)
        self.assert_(t.recognize(None) is None)

class SimpleTriggerTestCase(unittest.TestCase):

    def setUp(self):
        self.type = "CalExp"
        self.ids = [ IntegerIDFilter("visit", values=88), 
                     IntegerIDFilter("ccd", 0, 8), 
                     IntegerIDFilter("amp", 0, 16)       ]
        self.idd = {}
        for id in self.ids:
            self.idd[id.name] = id

    def tearDown(self):
        pass

    def testDatasetType(self, t=None):
        if not t:
            t = SimpleTrigger(self.type)

        ds = Dataset("goob")
        self.assert_(not t.recognize(ds))

        ds = Dataset(self.type)
        self.assert_(t.recognize(ds))

    def testDatasetType2(self, t=None):
        if not t:
            t = SimpleTrigger([self.type, "Decal"])

        ds = Dataset("goob")
        self.assert_(not t.recognize(ds))

        ds = Dataset(self.type)
        self.assert_(t.recognize(ds))
        ds = Dataset("Decal")
        self.assert_(t.recognize(ds))


    def testIds(self, t=None):
        if not t:
            t = SimpleTrigger(self.type, self.idd)
        
        ds = Dataset(self.type)
        self.assert_(not t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0)
        self.assert_(not t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0, visit=88)
        self.assert_(t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0, visit=88, filt='r')
        self.assert_(t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0, visit=89, filt='r')
        self.assert_(not t.recognize(ds))

    def testIds2(self, t=None):
        if not t:
            t = SimpleTrigger(ids=self.idd)
        
        ds = Dataset(self.type)
        self.assert_(not t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0)
        self.assert_(not t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0, visit=88)
        self.assert_(t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0, visit=88, filt='r')
        self.assert_(t.recognize(ds))

        ds = Dataset(self.type, ccd=5, amp=0, visit=89, filt='r')
        self.assert_(not t.recognize(ds))

    def testListDatasets(self):
        t = SimpleTrigger(self.type, self.idd)
        self.assert_(t.hasPredictableDatasetList())

        ds = Dataset(self.type, ccd=5, amp=0, visit=88, filt='r')
        dss = t.listDatasets(ds)
        self.assertEquals(len(dss), 8*16*1)
        self.assertEquals(dss[0].ids["visit"], 88)
        self.assert_(dss[0].ids.has_key("filt"))
        self.assertEquals(dss[0].ids["filt"], 'r')
        for ds in dss:
            self.assert_(t.recognize(ds), "failed to recognize " + str(ds))

        idd = dict(self.idd)
        idd["visit"] = IntegerIDFilter("visit", min=87)
        t = SimpleTrigger(self.type, idd)
        self.assert_(not t.hasPredictableDatasetList())
        dss = t.listDatasets(ds)
        self.assertEquals(len(dss), 8*16*1)
        self.assertEquals(dss[0].ids["visit"], 88)
        self.assert_(dss[0].ids.has_key("filt"))
        self.assertEquals(dss[0].ids["filt"], 'r')


    def testFromPolicy(self):
        p = Policy()
        p.set("datasetType", self.type)
        trig = SimpleTrigger.fromPolicy(p)
        self.testDatasetType(trig)

        trig = Trigger.fromPolicy(p)
        self.testDatasetType(trig)
        p.set("className", "Simple")
        trig = Trigger.fromPolicy(p)
        self.testDatasetType(trig)
        p.set("className", "SimpleTrigger")
        trig = Trigger.fromPolicy(p)
        self.testDatasetType(trig)

        p.set("className", "lsst.ctrl.sched.joboffice.goober.SimpleTrigger")
        self.assertRaises(ImportError, Trigger.fromPolicy, p)
        p.set("className", "Simple")

        idp = Policy()
        idp.set("name", "visit")
        idp.set("value", 88)
        p.add("id", idp)
        idp = Policy()
        idp.set("name", "ccd")
        idp.set("lim", 9)
        p.add("id", idp)
        idp = Policy()
        idp.set("name", "amp")
        idp.set("min", 0)
        idp.set("lim", 16)
        p.add("id", idp)

        trig = SimpleTrigger.fromPolicy(p)
        # pdb.set_trace()
        self.testIds(trig)

testds = Dataset("src",
                 ids={"visit": 85408535, "snap": 1, "raft": "4,3",
                      "sensor": "2,2", "channel": "1,7" })

class MapperTriggerTestCase(unittest.TestCase):

    def setUp(self):
        example = DefaultPolicyFile("ctrl_sched", "srcAssoc-joboffice.paf",
                                    "examples")
        self.schedpolicy = Policy.createPolicy(example)
        self.tpolicy = self.schedpolicy.get("schedule.trigger")
        self.trigger = None

    def tearDown(self):
        pass

    def testPolicyOK(self):
        self.assertEquals(self.tpolicy.get("className"), "MapperTrigger")

    def testFromPolicy(self):
        self.trigger = Trigger.fromPolicy(self.tpolicy)
        self.assert_(isinstance(self.trigger, MapperTrigger))

    def testRecognize(self):
        self.testFromPolicy()
        self.assert_(self.trigger.recognize(testds))

    def testNoRecognize(self):
        ds = copy.deepcopy(testds)
        ds.type = "goob"
        
        self.testFromPolicy()
        self.assert_(not self.trigger.recognize(ds))

    def testListJobs(self):
        self.testFromPolicy()
        jobs = self.trigger.listDatasets(testds)
        self.assertEquals(len(jobs), 1)
        self.assertEquals(jobs[0].type, 'source')
        self.assert_(jobs[0].ids.has_key('skyTile'))
        self.assertEquals(jobs[0].ids['skyTile'], 95127)
        
    def testListJobs2(self):
        policy = self.schedpolicy.get("schedule.job.input")
        trigger = Trigger.fromPolicy(policy)
        ds = Dataset("goob", ids={ "skyTile": 95127 })

        inputs = trigger.listDatasets(ds)
        self.assertEquals(len(inputs), 8)
        self.assertEquals(inputs[0].type, 'src')
        self.assert_(inputs[0].ids.has_key('visit'))
        self.assert_(inputs[0].ids.has_key('raft'))
        self.assert_(inputs[0].ids.has_key('sensor'))
        self.assertEquals(inputs[0].ids['visit'], 85408535)
        

__all__ = "AbstractTriggerTestCase SimpleTriggerTestCase".split()

if __name__ == "__main__":
    unittest.main()
