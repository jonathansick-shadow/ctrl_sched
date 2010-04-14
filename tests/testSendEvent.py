#!/usr/bin/env python
"""
Tests of the Dataset class
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.ctrl.events import EventSystem, EventReceiver

sendevent = os.path.join(os.environ["CTRL_SCHED_DIR"], "bin", "sendevent.py")
seargs = " -b %s -r %s -n %s -o %d %s %s %s"

esys = EventSystem.getDefaultEventSystem()
origid = esys.createOriginatorId()


class SendEventTestCase(unittest.TestCase):
    def setUp(self):
        self.topic = "test"
        self.broker = "lsst8.ncsa.uiuc.edu"
        self.runid = "test1"
        
    def tearDown(self):
        pass

    def testReady(self):
        rcvr = EventReceiver(self.broker, self.topic,
                             "RUNID='goob'")
        args = seargs % (self.broker, self.runid, "testPipe", origid, "ready", 
                         self.topic, "testPipe")
        print sendevent+args
        os.system(sendevent+args)
        event = rcvr.receiveEvent(500)
        self.assert_(event is None)

        rcvr = EventReceiver(self.broker, self.topic,
                             "RUNID='%s'" % self.runid)
        os.system(sendevent+args)
        event = rcvr.receiveEvent(500)
        self.assert_(event is not None, "generic event not selected")
        os.system(sendevent+args)
        event = rcvr.receiveStatusEvent(500)
        self.assert_(event is not None,  "failed to cast to status event")

        rcvr = EventReceiver(self.broker, self.topic,
                             "RUNID='%s' and STATUS='%s'" % (self.runid, "job:ready"))
        os.system(sendevent+args)
        event = rcvr.receiveStatusEvent(500)
        self.assert_(event is not None,  "status event not selected on status")

        rcvr = EventReceiver(self.broker, self.topic,
                             "RUNID='%s' and STATUS='%s' and DESTINATIONID=%d" % (self.runid, "job:ready", origid))
        args = seargs % (self.broker, self.runid, "testPipe", origid, "assign",
                         self.topic, "testPipe")
        print sendevent+args
        os.system(sendevent+args)
        event = rcvr.receiveCommandEvent(500)
        self.assert_(event is not None,  "status event not selected on destination")



__all__ = "SendEventTestCase".split()

if __name__ == "__main__":
    unittest.main()
