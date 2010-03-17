#!/usr/bin/env python
"""
Tests of the BlackboardItem classes
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

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
        return bb.DataProductItem.createItem(name, type)

    def _jobItem(self, name, type=""):
        return bb.JobItem.createItem(name, type)

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
        



__all__ = "BlackboardTestCase".split()

if __name__ == "__main__":
    unittest.main()
