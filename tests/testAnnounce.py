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
Tests of the announceDataset.py script
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import re
import sys
import unittest
import time

from lsst.ctrl.events import EventSystem, EventReceiver, Event, CommandEvent
import lsst.ctrl.sched.utils as utils
from lsst.ctrl.sched import Dataset

announceDataset = os.path.join(os.environ["CTRL_SCHED_DIR"], "bin",
                               "announceDataset.py")
seargs = " -b %(broker)s -r %(runid)s -t %(topic)s"
seargs += " -q"

dsfile = os.path.join(os.environ["CTRL_SCHED_DIR"], "examples",
                      "datasetlist.txt")


class AnnounceTestCase(unittest.TestCase):

    def setUp(self):
        self.topic = "test"
        self.broker = "lsst8.ncsa.uiuc.edu"
        self.runid = "test1"
        self.rcvr = EventReceiver(self.broker, self.topic,
                                  "RUNID='%s'" % self.runid)
        self.ds = Dataset("PostISR", ids={"visit": "9999", "ccd": "22",
                                          "amp": "07", "snap": "0"})
        names = self.ds.ids.keys()
        names.sort()
        self.dsstr = self.ds.type
        for name in names:
            self.dsstr += " %s=%s" % (name, self.ds.ids[name])

    def tearDown(self):
        pass

    def testSimple(self):
        cmd = announceDataset
        cmd += seargs % {"runid": self.runid, "topic": self.topic,
                         "broker": self.broker}
        cmd += " -D '%s'" % self.dsstr

        os.system(cmd)

        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        dss = self.extractDatasets(event)
        self.assertEquals(len(dss), 1)
        self.assertEquals(dss[0], self.ds)

    def testDelim(self):
        ds = re.sub(r' +', '/', self.dsstr)
        cmdb = announceDataset
        cmdb += seargs % {"runid": self.runid, "topic": self.topic,
                          "broker": self.broker}
        cmdb += " -i /"
        cmd = cmdb + " -D '%s'" % ds

        os.system(cmd)

        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        dss = self.extractDatasets(event)
        self.assertEquals(len(dss), 1)
        self.assertEquals(dss[0], self.ds)

        ds = re.sub(r'=', ': ', ds)
        cmd = cmdb + (" -D '%s'" % ds)
        cmd += " -e ': '"

        os.system(cmd)

        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        dss = self.extractDatasets(event)
        self.assertEquals(len(dss), 1)
        self.assertEquals(dss[0], self.ds)

    def testInterval(self):
        cmd = "announceDataset.py"
        cmd += seargs % {"runid": self.runid, "topic": self.topic,
                         "broker": self.broker}
        cmd += " -I 2"  # pause 4 seconds
        cmd = cmd.split()
        cmd.append("-D")
        cmd.append(self.dsstr)

        os.spawnv(os.P_NOWAIT, announceDataset, cmd)
        event = self.rcvr.receiveEvent(0)
        self.assert_(event is None)

        event = self.rcvr.receiveEvent(5000)
        self.assert_(event is not None)
        dss = self.extractDatasets(event)
        self.assertEquals(len(dss), 1)
        self.assertEquals(dss[0], self.ds)

    def testFail(self):
        cmd = announceDataset
        cmd += seargs % {"runid": self.runid, "topic": self.topic,
                         "broker": self.broker}
        cmd += " -D '%s'" % self.dsstr
        cmd += " -f"

        os.system(cmd)

        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        dss = self.extractDatasets(event)
        self.assertEquals(len(dss), 1)
        self.assert_(not dss[0].valid)
        self.assertEquals(dss[0], self.ds)

    def testFormat(self):
        cmd = announceDataset
        cmd += seargs % {"runid": self.runid, "topic": self.topic,
                         "broker": self.broker}
        cmd += " -F '%s'" % "%(type)s-v%(visit)i-c%(ccd)s-a%(amp)s-s%(snap)i.fits"
        cmd += " -D '%s'" % "PostISR-v9999-c22-a07-s0.fits"

        ds = Dataset("PostISR", ids={"visit": 9999, "ccd": "22",
                                     "amp": "07", "snap": 0})
        os.system(cmd)

        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        dss = self.extractDatasets(event)
        self.assertEquals(len(dss), 1)
        self.assertEquals(dss[0], ds, "%s != %s" % (dss[0], ds))

    def testFile(self):
        ds = Dataset("PostISR", visit="888", ccd="10", amp="07", snap="0")

        cmd = "announceDataset.py"
        cmd += seargs % {"runid": self.runid, "topic": self.topic,
                         "broker": self.broker}
        cmd += " %s" % dsfile
        cmd = cmd.split()

        os.spawnv(os.P_NOWAIT, announceDataset, cmd)

        event = self.rcvr.receiveEvent(0)
        self.assert_(event is None)

        count = 0
        try:
            event = self.rcvr.receiveEvent(5000)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)

            ds.ids["amp"] = "08"
            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)

            ds.ids["amp"] = "09"
            ds.ids["visit"] = 888
            ds.ids["snap"] = 0
            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)

            ds.ids["visit"] = "888"
            ds.ids["snap"] = "0"
            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(dss[0].valid, "event #%i is not valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(not dss[0].valid, "failed event #%i is valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(not dss[0].valid, "failed event #%i is valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(dss[0].valid, "failed event #%i is valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(not dss[0].valid, "failed event #%i is valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(dss[0].valid, "failed event #%i is valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(not dss[0].valid, "failed event #%i is valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds)
            self.assert_(dss[0].valid, "failed event #%i is valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds,
                              "event #%i failed to use iddelim" % count)
            self.assert_(dss[0].valid, "event #%i is not valid" % count)

            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds,
                              "event #%i failed to use eqdelim" % count)

            ds.ids["visit"] = 888
            ds.ids["snap"] = 0
            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds,
                              "event #%i failed to use format: %s != %s" %
                              (count, dss[0], ds))

            ds.ids["snap"] = 1
            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds,
                              "event #%i failed to use format: %s != %s" %
                              (count, dss[0], ds))

            ds.ids["amp"] = "08"
            ds.ids["snap"] = 0
            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds,
                              "event #%i failed to use format: %s != %s" %
                              (count, dss[0], ds))

            ds.ids["amp"] = "08"
            ds.ids["snap"] = 1
            event = self.rcvr.receiveEvent(500)
            count += 1
            self.assert_(event is not None)
            dss = self.extractDatasets(event)
            self.assertEquals(len(dss), 1)
            self.assertEquals(dss[0], ds,
                              "event #%i failed to use format: %s != %s" %
                              (count, dss[0], ds))

            self.assertEquals(count, 17, "lost count of events")

        finally:
            for i in xrange(17-count):
                event = self.rcvr.receiveEvent(50)

    def extractDatasets(self, event):
        edss = event.getPropertySet().getArrayString("dataset")
        return utils.unserializeDatasetList(edss)

    def containsDataset(self, event, ds):
        edss = event.getPropertySet().getArrayString("dataset")
        for edsp in edss:
            eds = utils.unserializeDataset(edsp)
            if eds == ds:
                return True
        return False

    def testMax(self):
        cmd = announceDataset
        cmd += seargs % {"runid": self.runid, "topic": self.topic,
                         "broker": self.broker}

        max = " -m 3"
        dsopt = " %s" % dsfile

        os.system(cmd+max+dsopt)
        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is None)

        max = " -m 1"
        dsopt = ""
        dsopt += " -D '%s'" % self.dsstr
        dsopt += " -D '%s'" % self.dsstr

        os.system(cmd+max+dsopt)
        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is None)

        max = " -m 0"
        os.system(cmd+" -q"+max+dsopt)
        event = self.rcvr.receiveEvent(100)
        self.assert_(event is None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is None)

        max = " -m -4"
        os.system(cmd+max+dsopt)
        event = self.rcvr.receiveEvent(100)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is not None)

        max = " -m 3"
        os.system(cmd+max+dsopt)
        event = self.rcvr.receiveEvent(100)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is not None)

        dsopt += " %s" % dsfile
        os.system(cmd+max+dsopt)
        event = self.rcvr.receiveEvent(100)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is not None)
        event = self.rcvr.receiveEvent(50)
        self.assert_(event is None)


__all__ = "AnnounceTestCase".split()

if __name__ == "__main__":
    unittest.main()
