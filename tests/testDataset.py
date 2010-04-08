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

from lsst.ctrl.sched import Dataset
from lsst.pex.policy import Policy

class DatasetTestCase(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def testCtor(self):
        type = "CalExp"
        path = "goob/CalExp-v88-c12.fits"
        ccdid = 12
        visitid = 88

        ds = Dataset(type)
        self.assertEquals(ds.type, type)
        self.assert_(ds.path is None)
        self.assert_(ds.ids is None)

        ds = Dataset(type, path)
        self.assertEquals(ds.type, type)
        self.assertEquals(ds.path, path)
        self.assert_(ds.ids is None)

        ds = Dataset(type, ccdid=ccdid, visitid=visitid)
        self.assertEquals(ds.type, type)
        self.assert_(ds.path is None)
        self.assert_(ds.ids is not None)
        self.assertEquals(ds.ids["ccdid"], ccdid)
        self.assertEquals(ds.ids["visitid"], visitid)

        # pdb.set_trace()
        ds = Dataset(type, path, False, {"ccdid": ccdid, "visitid": visitid })
        self.assertEquals(ds.type, type)
        self.assertEquals(ds.path, path)
        self.assert_(not ds.valid)
        self.assert_(ds.ids is not None)
        self.assertEquals(ds.ids["ccdid"], ccdid)
        self.assertEquals(ds.ids["visitid"], visitid)

        ds = Dataset(type, ids={"ccdid": ccdid, "visitid": visitid })
        self.assertEquals(ds.type, type)
        self.assert_(ds.path is None)
        self.assert_(ds.ids is not None)
        self.assertEquals(ds.ids["ccdid"], ccdid)
        self.assertEquals(ds.ids["visitid"], visitid)

    def testToString(self):
        type = "CalExp"
        path = "goob/CalExp-v88-c12.fits"
        ccdid = 12
        visitid = 88

        ds = Dataset(type, ids={"ccdid": ccdid, "visitid": visitid })
        self.assertEquals(ds.toString(),
                          "%s-ccdid%s-visitid%s" % (type, ccdid, visitid))
        # print str(ds)

    def testFromPolicy(self):
        type = "CalExp"
        path = "goob/CalExp-v88-c12.fits"
        ccdid = 12
        visitid = 88

        p = Policy()
        p.set("type", type)
        # pdb.set_trace()
        ds = Dataset.fromPolicy(p)
        self.assertEquals(ds.type, type)
        self.assert_(ds.path is None)
        self.assert_(ds.ids is None)

        p.set("path", path)
        ds = Dataset.fromPolicy(p)
        self.assertEquals(ds.type, type)
        self.assertEquals(ds.path, path)
        self.assert_(ds.ids is None)

        p.set("ids.ccdid", ccdid)
        p.set("ids.visitid", visitid)
        ds = Dataset.fromPolicy(p)
        self.assertEquals(ds.type, type)
        self.assertEquals(ds.path, path)
        self.assert_(ds.ids is not None)
        self.assertEquals(ds.ids["ccdid"], ccdid)
        self.assertEquals(ds.ids["visitid"], visitid)

    def testToPolicy(self):
        type = "CalExp"
        path = "goob/CalExp-v88-c12.fits"
        ccdid = 12
        visitid = 88

        orig = Dataset(type, path, ccdid=ccdid, visitid=visitid)
        pol = orig.toPolicy()
        ds = Dataset.fromPolicy(pol)
        self.assertEquals(ds.type, type)
        self.assertEquals(ds.path, path)
        self.assert_(ds.ids is not None)
        self.assertEquals(ds.ids["ccdid"], ccdid)
        self.assertEquals(ds.ids["visitid"], visitid)

    def testEquals(self):
        type = "CalExp"
        path = "goob/CalExp-v88-c12.fits"
        ccdid = 12
        visitid = 88

        ds1 = Dataset(type, path, ccdid=ccdid, visitid=visitid)
        ds2 = Dataset(type, path, ccdid=ccdid, visitid=visitid)
        self.assert_(ds1 == ds2)
        self.assertEquals(ds1, ds2)
        self.assertEquals(ds2, ds1)
        self.assert_(ds1 in [ds2])

        ds2.ids["ccdid"] += 1
        self.assertNotEquals(ds1, ds2)
        self.assertNotEquals(ds2, ds1)
        self.assert_(ds1 not in [ds2])

        ds2 = Dataset(type, path, ccdid=ccdid, visitid=visitid, ampid=5)
        self.assertNotEquals(ds1, ds2)
        self.assertNotEquals(ds2, ds1)
        
        ds2 = Dataset("junk", path, ccdid=ccdid, visitid=visitid)
        self.assertNotEquals(ds1, ds2)
        self.assertNotEquals(ds2, ds1)

        ds2 = Dataset(type)
        self.assertNotEquals(ds1, ds2)
        self.assertNotEquals(ds2, ds1)
        
        ds2 = Dataset(None, ccdid=ccdid, visitid=visitid)
        self.assertNotEquals(ds1, ds2)
        self.assertNotEquals(ds2, ds1)
        ds1 = Dataset(None, ccdid=ccdid, visitid=visitid)
        self.assertEquals(ds1, ds2)
        self.assertEquals(ds2, ds1)
        


__all__ = "DatasetTestCase".split()

if __name__ == "__main__":
    unittest.main()
