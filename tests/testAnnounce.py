#!/usr/bin/env python
"""
Tests of the announceDataset.py script
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os, re
import sys
import unittest
import time

from lsst.ctrl.events import EventSystem, EventReceiver, Event, CommandEvent
import lsst.ctrl.sched.utils as utils
from lsst.ctrl.sched import Dataset

announceDataset = os.path.join(os.environ["CTRL_SCHED_DIR"], "bin",
                               "announceDataset.py")
seargs = " -b %(broker)s -r %(runid)s -t %(topic)s"
# seargs += " -q"

class AnnounceTestCase(unittest.TestCase):
    def setUp(self):
        self.topic = "test"
        self.broker = "lsst8.ncsa.uiuc.edu"
        self.runid = "test1"
        self.rcvr = EventReceiver(self.broker, self.topic,
                                  "RUNID='%s'" % self.runid)
        self.ds = Dataset("PostISR", ids={ "visit": "9999", "ccd": "22",
                                           "amp": "07", "snap":"0" })
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
                         "broker": self.broker }
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
                         "broker": self.broker }
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
                         "broker": self.broker }
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
                         "broker": self.broker }
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
                         "broker": self.broker }
        cmd += " -F '%s'" % "%(type)s-v%(visit)i-c%(ccd)s-a%(amp)s-s%(snap)i.fits"
        cmd += " -D '%s'" % "PostISR-v9999-c22-a07-s0.fits"

        ds = Dataset("PostISR", ids={ "visit": 9999, "ccd": "22",
                                      "amp": "07", "snap": 0 })
        os.system(cmd)

        event = self.rcvr.receiveEvent(500)
        self.assert_(event is not None)
        dss = self.extractDatasets(event)
        self.assertEquals(len(dss), 1)
        self.assertEquals(dss[0], ds, "%s != %s" % (dss[0], ds))

    def testFile(self):
        file = os.path.join(os.environ["CTRL_SCHED_DIR"], "examples",
                            "datasetlist.txt")
        ds = Dataset("PostISR", visit="888", ccd="10", amp="07", snap="0")
        
        cmd = "announceDataset.py"
        cmd += seargs % {"runid": self.runid, "topic": self.topic,
                         "broker": self.broker }
        cmd += " %s" % file
        cmd = cmd.split()

        os.spawnv(os.P_NOWAIT, announceDataset, cmd)

        event = self.rcvr.receiveEvent(0)
        self.assert_(event is None)

        count = 0
        try:
          event = self.rcvr.receiveEvent(5000); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)        

          ds.ids["amp"] = "08"
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)        
        
          ds.ids["amp"] = "09"
          ds.ids["visit"] = 888
          ds.ids["snap"] = 0
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)        
        
          ds.ids["visit"] = "888"
          ds.ids["snap"] = "0"
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)        
          self.assert_(dss[0].valid, "event #%i is not valid" % count)
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)
          self.assert_(not dss[0].valid, "failed event #%i is valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)
          self.assert_(not dss[0].valid, "failed event #%i is valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)
          self.assert_(dss[0].valid, "failed event #%i is valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)
          self.assert_(not dss[0].valid, "failed event #%i is valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)
          self.assert_(dss[0].valid, "failed event #%i is valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)
          self.assert_(not dss[0].valid, "failed event #%i is valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds)
          self.assert_(dss[0].valid, "failed event #%i is valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds,
                            "event #%i failed to use iddelim" % count)
          self.assert_(dss[0].valid, "event #%i is not valid" % count) 
        
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds,
                            "event #%i failed to use eqdelim" % count)
        
          ds.ids["visit"] = 888
          ds.ids["snap"] = 0
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds,
                            "event #%i failed to use format: %s != %s" %
                            (count, dss[0], ds))
        
          ds.ids["snap"] = 1
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds,
                            "event #%i failed to use format: %s != %s" %
                            (count, dss[0], ds))
        
          ds.ids["amp"] = "08"
          ds.ids["snap"] = 0
          event = self.rcvr.receiveEvent(500); count += 1
          self.assert_(event is not None)
          dss = self.extractDatasets(event)
          self.assertEquals(len(dss), 1)
          self.assertEquals(dss[0], ds,
                            "event #%i failed to use format: %s != %s" %
                            (count, dss[0], ds))
        
          ds.ids["amp"] = "08"
          ds.ids["snap"] = 1
          event = self.rcvr.receiveEvent(500); count += 1
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



__all__ = "AnnounceTestCase".split()

if __name__ == "__main__":
    unittest.main()
