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
Tests of the sendevent.py script
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.ctrl.events import EventSystem, EventReceiver, Event, CommandEvent

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

        selector = "%s='%s' and %s='%s' and %s=%d" % (Event.RUNID, self.runid, Event.STATUS, "job:assign", CommandEvent.DESTINATIONID, origid)
#        selector = "DESTINATIONID = %d" % (origid)
        print selector
        rcvr = EventReceiver(self.broker, self.topic, selector)
        args = seargs % (self.broker, self.runid, "testPipe", origid, "assign",
                         self.topic, "testPipe")
        print sendevent+args
        os.system(sendevent+args)
        event = rcvr.receiveCommandEvent(500)
        self.assert_(event is not None,  "status event not selected on destination")



__all__ = "SendEventTestCase".split()

if __name__ == "__main__":
    unittest.main()
