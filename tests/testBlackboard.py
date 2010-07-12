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
Tests of the Blackboard class
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.ctrl.sched import Dataset
import lsst.ctrl.sched.blackboard as bb

testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")

class BlackboardTestCase(unittest.TestCase):

    def setUp(self):
        self.bbdir = os.path.join(testdir,"testbb")
        self.bb = bb.Blackboard(self.bbdir)
        
        self.daq = os.path.join(self.bbdir,"dataAvailable")
        self.jsq = os.path.join(self.bbdir,"jobsPossible")
        self.jaq = os.path.join(self.bbdir,"jobsAvailable")
        self.jpq = os.path.join(self.bbdir,"jobsInProgress")
        self.jdq = os.path.join(self.bbdir,"jobsDone")
        self.prq = os.path.join(self.bbdir,"pipelinesReady")


    def tearDown(self):
        if os.path.exists(self.bbdir):
            if os.path.isdir(self.bbdir):
                for (dirpath, dirnames, filenames) \
                        in os.walk(self.bbdir, False):
                    for f in filenames:
                        os.remove(os.path.join(dirpath, f))
                    for d in dirnames:
                        os.rmdir(os.path.join(dirpath, d))
                os.rmdir(self.bbdir)
            else:
                os.remove(self.bbdir)

    def testEmpty(self):
        self.assert_(os.path.exists(self.bbdir))
        for d in "dataAvailable jobsPossible jobsAvailable jobsInProgress jobsDone pipelinesReady".split():
            path = os.path.join(self.bbdir,d)
            self.assert_(os.path.exists(path),
                         "queue directory not found: " + path)
            path = os.path.join(self.bbdir,d,"_order.list")
            self.assert_(os.path.exists(path),
                         "queue order file not found: " + path)

    def _datasetItem(self, name, type=""):
        ds = Dataset(type, name)
        return bb.DataProductItem.createItem(ds)

    def _jobItem(self, name, type=""):
        ds = Dataset(type)
        return bb.JobItem.createItem(ds, name)

    def testUnprotectedUpdates(self):
        item = self._datasetItem("v1234.fits", "raw")
        try:
            self.bb.queues.dataAvailable.append(item)
            self.fail("Unprotected queue access allowed")
        except AttributeError:
            pass
        
    def testAddDataset(self):
        item = self._datasetItem("v1234-s0.fits", "raw")
        with self.bb:
            self.bb.queues.dataAvailable.append(item)

            # query queue to confirm addition
            self.assertEquals(self.bb.queues.dataAvailable.length(), 1)
            self.assertEquals(self.bb.queues.dataAvailable.get(0).getName(),
                              "v1234-s0.fits")
        

        # confirm filesystem state
        itemfile = os.path.join(self.bbdir,"dataAvailable","v1234-s0.fits.paf")
        self.assert_(os.path.exists(itemfile))
        
    def testAddJob(self):
        item = self._jobItem("v1234")
        with self.bb:
            self.bb.queues.jobsPossible.append(item)

            # query queue to confirm addition
            self.assertEquals(self.bb.queues.jobsPossible.length(), 1)
            self.assertEquals(self.bb.queues.jobsPossible.get(0).getName(),
                              "v1234")
        
        # confirm filesystem state
        itemfile = os.path.join(self.jsq,"v1234.paf")
        self.assert_(os.path.exists(itemfile))
        
    def testMakeJobAvailable(self):

        # test first the transfer of a non-possible job
        item = self._jobItem("v1234")
        self.assertRaises(bb.BlackboardUpdateError,
                          self.bb.makeJobAvailable, item)

        # now test a normal transfer
        with self.bb:
            self.bb.queues.jobsPossible.append(item)

        self.bb.makeJobAvailable(item)

        with self.bb:
            # query queues to confirm transfer
            self.assertEquals(self.bb.queues.jobsPossible.length(), 0)
            self.assertEquals(self.bb.queues.jobsAvailable.length(), 1)
            self.assertEquals(self.bb.queues.jobsAvailable.get(0).getName(),
                              "v1234")

        # confirm filesystem state
        self.assert_(os.path.exists(os.path.join(self.jaq,"v1234.paf")))
        self.assert_(not os.path.exists(os.path.join(self.jsq,"v1234.paf")))
        
    def testAllocateNextJob(self):

        # test transfering a job from an empty jobsAvailable queue
        self.assertRaises(bb.EmptyQueueError, self.bb.allocateNextJob, 333L)

        # now test a normal transfer
        with self.bb:
            self.bb.queues.jobsAvailable.append(self._jobItem("v1234"))
            self.bb.queues.jobsAvailable.append(self._jobItem("v1235"))

        self.bb.allocateNextJob(1982349810931831L)

        with self.bb:
            # query queues to confirm transfer
            self.assertEquals(self.bb.queues.jobsAvailable.length(), 1)
            self.assertEquals(self.bb.queues.jobsInProgress.length(), 1)
            self.assertEquals(self.bb.queues.jobsInProgress.get(0).getName(),
                              "v1234")
            self.assertEquals(self.bb.queues.jobsAvailable.get(0).getName(),
                              "v1235")

        # confirm filesystem state
        self.assert_(os.path.exists(os.path.join(self.jpq,"v1234.paf")))
        self.assert_(os.path.exists(os.path.join(self.jaq,"v1235.paf")))
        self.assert_(not os.path.exists(os.path.join(self.jaq,"v1234.paf")))

        # transfer 2nd job
        self.bb.allocateNextJob(1982349810931831L)

        with self.bb:
            # query queues to confirm transfer
            self.assertEquals(self.bb.queues.jobsAvailable.length(), 0)
            self.assertEquals(self.bb.queues.jobsInProgress.length(), 2)
            self.assertEquals(self.bb.queues.jobsInProgress.get(0).getName(),
                              "v1234")
            self.assertEquals(self.bb.queues.jobsInProgress.get(1).getName(),
                              "v1235")

        # confirm filesystem state
        self.assert_(os.path.exists(os.path.join(self.jpq,"v1234.paf")))
        self.assert_(os.path.exists(os.path.join(self.jpq,"v1235.paf")))
        self.assert_(not os.path.exists(os.path.join(self.jaq,"v1234.paf")))
        self.assert_(not os.path.exists(os.path.join(self.jaq,"v1235.paf")))
        
    def testMakeJobDone(self):

        # test first the transfer of a non-possible job
        item = self._jobItem("v1234")
        self.assertRaises(bb.BlackboardUpdateError,
                          self.bb.markJobDone, item)

        # now test a normal transfer
        with self.bb:
            self.bb.queues.jobsInProgress.append(item)

        self.bb.markJobDone(item)

        with self.bb:
            # query queues to confirm transfer
            self.assertEquals(self.bb.queues.jobsInProgress.length(), 0)
            self.assertEquals(self.bb.queues.jobsDone.length(), 1)
            self.assertEquals(self.bb.queues.jobsDone.get(0).getName(),
                              "v1234")

        # confirm filesystem state
        self.assert_(os.path.exists(os.path.join(self.jdq,"v1234.paf")))
        self.assert_(not os.path.exists(os.path.join(self.jpq,"v1234.paf")))
        
        




__all__ = "BlackboardTestCase".split()

if __name__ == "__main__":
    unittest.main()
