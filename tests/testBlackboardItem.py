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
from lsst.ctrl.sched import Dataset
from lsst.pex.policy import Policy, PAFWriter

testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")

class AbsBBItemTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testNoConstruction(self):
        self.assertRaises(RuntimeError, bb.BlackboardItem)

    def testAbsMeth(self):
        bbi = bb.BlackboardItem(True)
        self.assertRaises(RuntimeError, bbi.getProperty, "goofy")
        self.assertRaises(RuntimeError, bbi.getPropertyNames)
        self.assertRaises(RuntimeError, bbi.keys)
        self.assertRaises(RuntimeError, bbi.hasProperty, "goofy")
        self.assertRaises(RuntimeError, bbi.has_key, "goofy")

class DictBBItemTestCase(unittest.TestCase):

    def setUp(self):
        self.bbi = bb.DictBlackboardItem({"foo": "bar", "count": 3,
                                          "files": [ "goob", "gurn"] })
        self.initCount = 3

    def tearDown(self):
        pass

    def testEmptyCtr(self):
        bbi = bb.DictBlackboardItem()
        self.assertEquals(len(bbi.getPropertyNames()), 0)

    def testGetProp(self):
        self.assertEquals(self.bbi.getProperty("foo"), "bar")
        self.assertEquals(self.bbi.getProperty("foo", 5), "bar")
        self.assertEquals(self.bbi.getProperty("count", 5), 3)
        self.assertEquals(self.bbi.getProperty("files"), ["goob", "gurn"])
        self.assertEquals(self.bbi.getProperty("goob", 5), 5)
        self.assert_(self.bbi.getProperty("goob") is None)

    def testSeqAccess(self):
        self.assertEquals(self.bbi["foo"], "bar")
        self.assertEquals(self.bbi["count"], 3)
        self.assertEquals(self.bbi["files"], ["goob", "gurn"])
        self.assertEquals(self.bbi.getProperty("goob", 5), 5)
        self.assert_(self.bbi.getProperty("goob") is None)
        
    def testSetProp(self):
        self.bbi._setProperty("henry", "hank")
        self.assertEquals(self.bbi.getProperty("henry"), "hank")
        self.bbi._setProperty("seq", range(3))
        self.assertEquals(self.bbi.getProperty("seq"), [0, 1, 2])

    def testGetPropertyNames(self):
        names = self.bbi.getPropertyNames()
        self.assertEquals(len(names), self.initCount)
        self.assert_("foo" in names)
        self.assert_("count" in names)
        self.assert_("files" in names)
        self.assert_("goob" not in names)

    def testKeys(self):
        names = self.bbi.keys()
        self.assertEquals(len(names), 3)
        self.assert_("foo" in names)
        self.assert_("count" in names)
        self.assert_("files" in names)
        self.assert_("goob" not in names)

class PolicyBBItemTestCase(DictBBItemTestCase):

    propfile = os.path.join(testdir, "props.paf")
    tmppropfile = os.path.join(testdir, "tmpprops.paf")
    
    def setUp(self):
        if not os.path.exists(self.propfile):
            p = Policy()
            p.set("foo", "bar")
            p.set("count", 3)
            p.set("files", "goob")
            p.add("files", "gurn")
            w = PAFWriter(self.propfile)
            w.write(p, True)
            w.close()
        
        self.bbi = bb.PolicyBlackboardItem(self.propfile)

        self.initCount = 3

    def tearDown(self):
        if os.path.exists(self.tmppropfile):
            os.remove(self.tmppropfile)

    # inherits all tests from DictBBItemTestCase

    def testEmptyCtr(self):
        # overriding DictBBItemTestCase
        bbi = bb.PolicyBlackboardItem()
        self.assertEquals(len(bbi.getPropertyNames()), 0)

    def testCopyFrom(self):
        # create empty item
        bbi = bb.PolicyBlackboardItem()
        self.assertEquals(len(bbi.getPropertyNames()), 0)

        # test copy
        bbi._copyFrom(self.bbi)
        self.assertEquals(len(bbi.getPropertyNames()), 3)
        self.assertEquals(bbi.getProperty("foo"), "bar")

        # test that copy doesn't affect original
        bbi._setProperty("foo", "hank")
        self.assertEquals(bbi.getProperty("foo"), "hank")
        self.assertEquals(self.bbi.getProperty("foo"), "bar")

    def testFormatter(self):
        fmtr = self.bbi.createFormatter()
        fmtr = bb.PolicyBlackboardItem.createFormatter()
        self.assert_(fmtr is not None)
        self.assert_(hasattr(fmtr, "write"))
        self.assert_(hasattr(fmtr, "openItem"))

        self.assert_(not os.path.exists(self.tmppropfile))
        fmtr.write(self.tmppropfile, self.bbi)
        self.assert_(os.path.exists(self.tmppropfile))
        
        del self.bbi
        self.bbi = fmtr.openItem(self.tmppropfile)
        self.testGetProp()
        
class ImplBBItemTestCase1(DictBBItemTestCase):

    def setUp(self):
        self.bbi = bb.ImplBlackboardItem(
            bb.DictBlackboardItem({"foo": "bar", "count": 3,
                                          "files": [ "goob", "gurn"] }))
        self.initCount = 3
        
    def tearDown(self):
        pass

    # inherits all tests from DictBBItemTestCase

class ImplBBItemTestCase2(ImplBBItemTestCase1):

    def setUp(self):
        p = Policy()
        p.set("foo", "bar")
        p.set("count", 3)
        p.set("files", "goob")
        p.add("files", "gurn")
        impl = bb.PolicyBlackboardItem()
        impl._props = p
        self.bbi = bb.ImplBlackboardItem(impl)
            
        self.initCount = 3
        
    def tearDown(self):
        pass

    # inherits all tests from DictBBItemTestCase

class BasicBBItemTestCase1(ImplBBItemTestCase1):

    def setUp(self):
        self.name = "Goob"
        self.bbi = bb.BasicBlackboardItem(
            bb.DictBlackboardItem({"foo": "bar", "count": 3,
                                          "files": [ "goob", "gurn"] }),
            self.name)

        self.initCount = 4
        
    def tearDown(self):
        pass

    # inherits all tests from DictBBItemTestCase
    
    def testNameSet(self):
        self.assertEquals(self.bbi.getProperty("NAME"), self.name)

    def testGetPropertyNames(self):
        names = self.bbi.getPropertyNames()
        self.assertEquals(len(names), self.initCount)
        self.assert_("foo" in names)
        self.assert_("count" in names)
        self.assert_("files" in names)
        self.assert_("NAME" in names)
        self.assert_("goob" not in names)

    def testKeys(self):
        names = self.bbi.keys()
        self.assertEquals(len(names), self.initCount)
        self.assert_("foo" in names)
        self.assert_("count" in names)
        self.assert_("files" in names)
        self.assert_("NAME" in names)
        self.assert_("goob" not in names)

class BasicBBItemTestCase2(BasicBBItemTestCase1):

    def setUp(self):
        self.name = "Goob"
        self.bbi = bb.BasicBlackboardItem.createItem(self.name,
                                                 {"foo": "bar", "count": 3,
                                                  "files": [ "goob", "gurn"] })
        
        self.initCount = 4
        
    def tearDown(self):
        pass

    # inherits all tests from DictBBItemTestCase
    
class DataProdBBItemTestCase(BasicBBItemTestCase1):

    def setUp(self):
        type = "CalExp"
        path = "goob/CalExp-v88-c12.fits"
        ccdid = 12
        visitid = 88
        ds = Dataset(type, path, ccdid=ccdid, visitid=visitid)
        self.name = ds.toString()

        self.bbi = bb.DataProductItem.createItem(dataset=ds,
                                           props={"foo": "bar", "count": 3,
                                                  "files": [ "goob", "gurn"] })
        self.initCount = 12
        
    def tearDown(self):
        pass

    # inherits all tests from DictBBItemTestCase

    def testStdNames(self):
        self.assert_(self.bbi[bb.Props.SUCCESS])
        self.assert_(self.bbi.hasProperty(bb.Props.DATASET))
        ds = self.bbi.getProperty(bb.Props.DATASET)
        self.assert_(isinstance(ds, Policy))
        self.assertEquals(ds.get("type"), "CalExp")
        ids = ds.get("ids")
        self.assert_(ids.exists("visitid"))
        self.assert_(ids.exists("ccdid"))

    def testAccessors(self):
        ds = self.bbi.getDataset()
        self.assert_(isinstance(ds, Dataset))
        self.assertEquals(ds.type, "CalExp")
        self.assert_(ds.ids.has_key("visitid"))
        self.assert_(ds.ids.has_key("ccdid"))
        self.assertEquals(ds.ids["visitid"], 88)
        self.assertEquals(ds.ids["ccdid"], 12)

__all__ = "AbsBBItemTestCase DictBBItemTestCase PolicyBBItemTestCase ImplBBItemTestCase1".split()

if __name__ == "__main__":
    unittest.main()
