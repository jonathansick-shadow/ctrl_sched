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
Tests of the SharedData class
"""
from __future__ import with_statement

import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

import lsst.ctrl.sched.blackboard as bb
import lsst.ctrl.sched.blackboard.queue as bbq
from lsst.ctrl.sched.blackboard.queue import _FSDBBlackboardQueue
from lsst.ctrl.sched.blackboard.queue import _PolicyBlackboardQueue
from lsst.pex.policy import Policy, PAFWriter

testdir = os.path.join(os.environ["CTRL_SCHED_DIR"], "tests")


class AbsBBItemQTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testNoConstruction(self):
        self.assertRaises(RuntimeError, bb.BlackboardItemQueue)

    def testAbsMeth(self):
        q = bb.BlackboardItemQueue(True)
        self.assertRaises(RuntimeError, q.length)
        self.assertRaises(RuntimeError, q.isEmpty)
        self.assertRaises(RuntimeError, q.get, 0)
        self.assertRaises(RuntimeError, q.pop)
        self.assertRaises(RuntimeError, q.append, None)
        self.assertRaises(RuntimeError, q.insertAt, None)
        self.assertRaises(RuntimeError, q.insert, None)
        self.assertRaises(RuntimeError, q.transferNextTo, q, 0)
        self.assertRaises(RuntimeError, q.iterate)


class FSQueueFailBaseTestCase(unittest.TestCase):

    def setUp(self):
        self.dbdir = os.path.join(testdir, "testqueue")
        self.fmtr = bb.PolicyBlackboardItem.createFormatter()

    def tearDown(self):
        if os.path.exists(self.dbdir):
            if os.path.isdir(self.dbdir):
                files = os.listdir(self.dbdir)
                for f in files:
                    os.remove(os.path.join(self.dbdir, f))
                os.rmdir(self.dbdir)
            else:
                os.remove(self.dbdir)

    def testPreExist(self):
        self.assert_(not os.path.exists(self.dbdir), "%s: exists" % self.dbdir)
        fd = open(self.dbdir, "w")
        print >> fd, "boom"
        fd.close()

        self.assertRaises(bb.BlackboardAccessError, _FSDBBlackboardQueue,
                          self.dbdir,
                          bb.PolicyBlackboardItem.createFormatter())

    def testNoParent(self):
        self.assertRaises(bb.BlackboardAccessError, _FSDBBlackboardQueue,
                          "/goober/miser",
                          bb.PolicyBlackboardItem.createFormatter())


class FSQueueTestCase(unittest.TestCase):

    def setUp(self):
        # pdb.set_trace()
        self.dbdir = os.path.join(testdir, "testqueue")
        self.q = _PolicyBlackboardQueue(self.dbdir)

    def _newItem(self, data=None):
        out = bb.PolicyBlackboardItem()
        if data:
            for key in data.keys():
                out._setProperty(key, data[key])
        return out

    def tearDown(self):
        del self.q
        if os.path.exists(self.dbdir):
            if os.path.isdir(self.dbdir):
                files = os.listdir(self.dbdir)
                for f in files:
                    os.remove(os.path.join(self.dbdir, f))
                os.rmdir(self.dbdir)
            else:
                os.remove(self.dbdir)

    def testEmpty(self):
        self.assertEquals(self.q.length(), 0)
        self.assert_(self.q.isEmpty())

    def testFilename(self):
        filename = self.q.filenameFor(self._newItem({}))
        self.assertEquals(filename, "unknown.paf")
        path = os.path.join(self.dbdir, filename)
        f = open(path, "w")
        with f:
            print >> f, "boo"
        filename = self.q.filenameFor(self._newItem({}))
        self.assertEquals(filename, "unknown.1.paf")
        filename = self.q.filenameFor(self._newItem({"NAME": "goob"}))
        self.assertEquals(filename, "goob.paf")

    def testPendingName(self):
        filename = self.q.filenameFor(self._newItem())
        pending = self.q.pendingAddFor(filename)
        self.assertEquals(pending, ".add."+filename)

        pending = os.path.join(self.dbdir, pending)
        f = open(pending, "w")
        with f:
            print >> f, "boo"
        pending = self.q.pendingAddFor(filename)
        self.assertEquals(pending, ".add.1."+filename)

        pending = self.q.pendingDelFor(filename)
        self.assertEquals(pending, ".del."+filename)

        pending = os.path.join(self.dbdir, pending)
        f = open(pending, "w")
        with f:
            print >> f, "boo"
        pending = self.q.pendingDelFor(filename)
        self.assertEquals(pending, ".del.1."+filename)

    def testAppend(self):
        self.q.append(self._newItem({"panel": 1, "foo": "bar"}))
        self.assertEquals(self.q.length(), 1)
        self.assert_(not self.q.isEmpty())

        self.assert_(os.path.exists(os.path.join(self.dbdir, "_order.list")))
        self.assert_(os.path.exists(os.path.join(self.dbdir, "unknown.paf")))

        self.q.append(self._newItem({"panel": 1, "foo": "bar"}))
        self.assertEquals(self.q.length(), 2)
        self.assert_(os.path.exists(os.path.join(self.dbdir, "unknown.1.paf")))

        self.q.append(self._newItem({"panel": 1, "NAME": "bar"}))
        self.assertEquals(self.q.length(), 3)
        self.assert_(os.path.exists(os.path.join(self.dbdir, "bar.paf")))

        files = self.q._sd.files
        self.assertEquals(files[0], "unknown.paf")
        self.assertEquals(files[1], "unknown.1.paf")
        self.assertEquals(files[2], "bar.paf")
        files = self.q._loadOrder()
        self.assertEquals(files[0], "unknown.paf")
        self.assertEquals(files[1], "unknown.1.paf")
        self.assertEquals(files[2], "bar.paf")


class InMemoryBBQueueTestCase(unittest.TestCase):

    def setUp(self):
        # pdb.set_trace()
        self.q = bbq.InMemoryBlackboardQueue()

    def _newItem(self, name, data=None):
        return bb.BasicBlackboardItem.createItem(name, data)

    def testEmpty(self):
        self.assertEquals(self.q.length(), 0)
        self.assert_(self.q.isEmpty())

    def testAppend(self):
        self.q.append(self._newItem("item1"))
        self.assertEquals(self.q.length(), 1)
        self.assert_(not self.q.isEmpty())

    def testGet(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))

        item = self.q.get(1)
        self.assertEquals(item["NAME"], "item2")
        self.assertEquals(item["pos"], 2)
        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)

    def testPop0(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.assertEquals(self.q.length(), 2)

        item = self.q.pop()
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)
        self.assertEquals(self.q.length(), 1)

        deleted = None
        if hasattr(item, "filename"):
            deleted = item.filename
            self.assertEquals(deleted,
                              os.path.join(self.dbdir, ".del.item1.paf"))
            self.assert_(os.path.exists(deleted))

        item = self.q.get(0)
        if deleted:
            self.assert_(not os.path.exists(deleted))
        self.assertEquals(item["NAME"], "item2")
        self.assertEquals(item["pos"], 2)

        item = self.q.pop(0)
        self.assertEquals(item["NAME"], "item2")
        self.assertEquals(item["pos"], 2)
        self.assertEquals(self.q.length(), 0)

    def testPopMid(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        item = self.q.pop(1)
        self.assertEquals(item["NAME"], "item2")
        self.assertEquals(item["pos"], 2)
        self.assertEquals(self.q.length(), 2)

        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)
        item = self.q.get(1)
        self.assertEquals(item["NAME"], "item3")
        self.assertEquals(item["pos"], 3)

    def testPopEnd(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.assertEquals(self.q.length(), 2)

        item = self.q.pop(1)
        self.assertEquals(item["NAME"], "item2")
        self.assertEquals(item["pos"], 2)
        self.assertEquals(self.q.length(), 1)

        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)

    def testBadPop(self):
        self.assertRaises(IndexError, self.q.pop)

        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))

        self.assertRaises(IndexError, self.q.pop, 2)
        self.assertRaises(IndexError, self.q.pop, 3)

    def testInsertAt0(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.assertEquals(self.q.length(), 2)

        self.q.insertAt(self._newItem("item3", {"pos": 3}), 0)
        self.assertEquals(self.q.length(), 3)

        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item3")
        self.assertEquals(item["pos"], 3)
        item = self.q.get(1)
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)

    def testInsertAtEmpty(self):
        self.assertEquals(self.q.length(), 0)

        self.q.insertAt(self._newItem("item3", {"pos": 3}), 0)
        self.assertEquals(self.q.length(), 1)

        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item3")
        self.assertEquals(item["pos"], 3)

    def testInsertAtMid(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.assertEquals(self.q.length(), 2)

        self.q.insertAt(self._newItem("item3", {"pos": 3}), 1)
        self.assertEquals(self.q.length(), 3)

        item = self.q.get(1)
        self.assertEquals(item["NAME"], "item3")
        self.assertEquals(item["pos"], 3)
        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)

    def testInsertAtEnd(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.assertEquals(self.q.length(), 2)

        self.q.insertAt(self._newItem("item3", {"pos": 3}), -1)
        self.assertEquals(self.q.length(), 3)

        item = self.q.get(2)
        self.assertEquals(item["NAME"], "item3")
        self.assertEquals(item["pos"], 3)
        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)
        item = self.q.get(1)
        self.assertEquals(item["NAME"], "item2")
        self.assertEquals(item["pos"], 2)

        self.q.insertAt(self._newItem("item3", {"pos": 3}), 10)
        self.assertEquals(self.q.length(), 4)

        item = self.q.get(3)
        self.assertEquals(item["NAME"], "item3")
        self.assertEquals(item["pos"], 3)
        item = self.q.get(2)
        self.assertEquals(item["NAME"], "item3")
        self.assertEquals(item["pos"], 3)
        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item1")
        self.assertEquals(item["pos"], 1)
        item = self.q.get(1)
        self.assertEquals(item["NAME"], "item2")
        self.assertEquals(item["pos"], 2)

    def testInsert(self):
        self.q.insert(self._newItem("item1"))
        self.assertEquals(self.q.length(), 1)

        item = self.q.get(0)
        self.assertEquals(item["NAME"], "item1")

        self.q.insert(self._newItem("item2"), 3)
        self.assertEquals(self.q.length(), 2)
        item = self.q.get(1)
        self.assertEquals(item["NAME"], "item2")

    def testIterate(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        i = 0
        for item in self.q.iterate():
            i += 1
            self.assertEquals(item["pos"], i)
        self.assertEquals(i, 3)

    def testTransferFromEmpty(self):
        other = bbq.InMemoryBlackboardQueue()
        self.assertRaises(bb.EmptyQueueError, self.q.transferNextTo, other)

    def testTransfer(self):
        other = bbq.InMemoryBlackboardQueue()

        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        self.q.transferNextTo(other, 3)
        self.assertEquals(self.q.length(), 2)
        self.assertEquals(self.q.get(0)["pos"], 2)

        self.assertEquals(other.length(), 1)
        self.assertEquals(other.get(0)["pos"], 1)

        self.q.transferNextTo(other, 3)
        self.assertEquals(self.q.length(), 1)
        self.assertEquals(self.q.get(0)["pos"], 3)

        self.assertEquals(other.length(), 2)
        self.assertEquals(other.get(0)["pos"], 1)
        self.assertEquals(other.get(1)["pos"], 2)

        self.q.transferNextTo(other, 3)
        self.assertEquals(self.q.length(), 0)

        self.assertEquals(other.length(), 3)
        self.assertEquals(other.get(0)["pos"], 1)
        self.assertEquals(other.get(1)["pos"], 2)
        self.assertEquals(other.get(2)["pos"], 3)


class FSQueueBaseTestCase(InMemoryBBQueueTestCase):

    def setUp(self):
        # pdb.set_trace()
        self.dbdir = os.path.join(testdir, "testqueue")
        self.q = _PolicyBlackboardQueue(self.dbdir)

    def tearDown(self):
        del self.q
        for dir in (self.dbdir, self.dbdir+"2"):
            if os.path.exists(dir):
                if os.path.isdir(dir):
                    files = os.listdir(dir)
                    for f in files:
                        os.remove(os.path.join(dir, f))
                    os.rmdir(dir)
                else:
                    os.remove(dir)

    def testAppend(self):
        self.q.append(self._newItem("item1"))
        self.assertEquals(self.q.length(), 1)
        self.assert_(not self.q.isEmpty())

        self.assert_(os.path.exists(os.path.join(self.dbdir, "_order.list")))
        self.assert_(os.path.exists(os.path.join(self.dbdir, "item1.paf")))

        self.q.append(self._newItem("item1", {"panel": 1, "foo": "bar"}))
        self.assertEquals(self.q.length(), 2)
        self.assert_(os.path.exists(os.path.join(self.dbdir, "item1.1.paf")))

        self.q.append(self._newItem("item3", {"panel": 1, "foo": "bar"}))
        self.assertEquals(self.q.length(), 3)
        self.assert_(os.path.exists(os.path.join(self.dbdir, "item3.paf")))

        if hasattr(self.q, "_sd"):
            files = self.q._sd.files
            self.assertEquals(files[0], "item1.paf")
            self.assertEquals(files[1], "item1.1.paf")
            self.assertEquals(files[2], "item3.paf")
            files = self.q._loadOrder()
            self.assertEquals(files[0], "item1.paf")
            self.assertEquals(files[1], "item1.1.paf")
            self.assertEquals(files[2], "item3.paf")

    def testTransfer(self):
        self.assert_(not os.path.exists(self.dbdir+"2"))
        other = _PolicyBlackboardQueue(self.dbdir+"2")
        self.assert_(os.path.exists(self.dbdir+"2"))

        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        self.q.transferNextTo(other, 3)
        self.assertEquals(self.q.length(), 2)
        self.assertEquals(self.q.get(0)["pos"], 2)

        self.assertEquals(other.length(), 1)
        self.assertEquals(other.get(0)["pos"], 1)

        self.q.transferNextTo(other, 3)
        self.assertEquals(self.q.length(), 1)
        self.assertEquals(self.q.get(0)["pos"], 3)

        self.assertEquals(other.length(), 2)
        self.assertEquals(other.get(0)["pos"], 1)
        self.assertEquals(other.get(1)["pos"], 2)

        self.q.transferNextTo(other, 3)
        self.assertEquals(self.q.length(), 0)

        self.assertEquals(other.length(), 3)
        self.assertEquals(other.get(0)["pos"], 1)
        self.assertEquals(other.get(1)["pos"], 2)
        self.assertEquals(other.get(2)["pos"], 3)

    def testReconstitute(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        del self.q
        self.setUp()
        self.assertEquals(self.q.length(), 3)
        self.assertEquals(self.q.get(0)["pos"], 1)
        self.assertEquals(self.q.get(1)["pos"], 2)
        self.assertEquals(self.q.get(2)["pos"], 3)


class TransactionalBBQueueTestCase(FSQueueBaseTestCase):

    def setUp(self):
        # pdb.set_trace()
        self.dbdir = os.path.join(testdir, "testqueue")
        persistq = _PolicyBlackboardQueue(self.dbdir)
        self.q = bbq.TransactionalBlackboardQueue(persistq)

    def testTransaction(self):
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        # pdb.set_trace()
        with self.q:
            self.assertEquals(self.q._rbq.length(), 3)

            item = self.q.pop(0)
            self.assertEquals(self.q.length(), 2)
            self.assertEquals(self.q._rbq.length(), 3)
            self.assertEquals(item.getName(), "item1")

            self.q.insertAt(self._newItem("item2a", {"pos": 2}), 1)
            self.assertEquals(self.q.length(), 3)
            self.q.insert(self._newItem("item4", {"pos": 4}), 10)
            self.assertEquals(self.q.length(), 4)

        self.assertEquals(self.q.length(), 4)
        self.assertEquals(self.q.get(0).getName(), "item2")
        self.assertEquals(self.q.get(1).getName(), "item2a")
        self.assertEquals(self.q.get(3).getName(), "item4")
        self.assert_(self.q._rbq is None)

        # make sure that the state of the data on disk is consistent
        self.assertEquals(self.q._dskq.length(), 4)
        self.assertEquals(self.q._dskq.get(0).getProperty("NAME"), "item2")
        self.assertEquals(self.q._dskq.get(1).getProperty("NAME"), "item2a")
        self.assertEquals(self.q._dskq.get(3).getProperty("NAME"), "item4")

    def testRollback1(self):
        """
        test rolling back from a non-commit error within the transaction
        """
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        # pdb.set_trace()
        try:
            with self.q:
                self.assertEquals(self.q._rbq.length(), 3)

                item = self.q.pop(0)
                self.assertEquals(self.q.length(), 2)
                self.assertEquals(self.q._rbq.length(), 3)
                self.assertEquals(item.getName(), "item1")

                self.q.insertAt(self._newItem("item2a", {"pos": 2}), 1)
                self.assertEquals(self.q.length(), 3)
                self.q.insert(self._newItem("item4", {"pos": 4}), 10)
                self.assertEquals(self.q.length(), 4)
                raise RuntimeError("testing rollback")
        except RuntimeError:
            pass

        self.assertEquals(self.q.length(), 3)
        self.assertEquals(self.q.get(0).getName(), "item1")
        self.assertEquals(self.q.get(1).getName(), "item2")
        self.assertEquals(self.q.get(2).getName(), "item3")
        self.assert_(self.q._rbq is None)

        # make sure that the state of the data on disk is consistent
        self.assertEquals(self.q._dskq.length(), 3)
        self.assertEquals(self.q._dskq.get(0).getProperty("NAME"), "item1")
        self.assertEquals(self.q._dskq.get(1).getProperty("NAME"), "item2")
        self.assertEquals(self.q._dskq.get(2).getProperty("NAME"), "item3")

    def testRollback2(self):
        """
        test rolling back from a commit error within the transaction
        """
        self.q.append(self._newItem("item1", {"pos": 1}))
        self.q.append(self._newItem("item2", {"pos": 2}))
        self.q.append(self._newItem("item3", {"pos": 3}))
        self.assertEquals(self.q.length(), 3)

        # pdb.set_trace()
        dbdir = self.q._dskq._dbdir
        try:
            with self.q:
                self.assertEquals(self.q._rbq.length(), 3)

                item = self.q.pop(0)
                self.assertEquals(self.q.length(), 2)
                self.assertEquals(self.q._rbq.length(), 3)
                self.assertEquals(item.getName(), "item1")

                self.q.insertAt(self._newItem("item2a", {"pos": 2}), 1)
                self.assertEquals(self.q.length(), 3)
                self.q.insert(self._newItem("item4", {"pos": 4}), 10)
                self.assertEquals(self.q.length(), 4)

                # corrupt the internal data so that the disk commit fails
                os.remove(os.path.join(dbdir, "item1.paf"))
        except Exception, ex:
            self.assert_(isinstance(ex, OSError),
                         "unexpected error: " + str(ex))
            self.q._dskq._dbdir = dbdir
            dbdir = None
        self.assert_(dbdir is None)

        self.assertEquals(self.q.length(), 3)
        self.assertEquals(self.q.get(0).getName(), "item1")
        self.assertEquals(self.q.get(1).getName(), "item2")
        self.assertEquals(self.q.get(2).getName(), "item3")
        self.assert_(self.q._rbq is None)

        # make sure that the state of the data on disk is consistent
        self.assertEquals(self.q._dskq.length(), 3)
        self.assertEquals(self.q._dskq.get(0).getProperty("NAME"), "item1")
        self.assertEquals(self.q._dskq.get(1).getProperty("NAME"), "item2")
        self.assertEquals(self.q._dskq.get(2).getProperty("NAME"), "item3")


class PolicyBBQueueTestCase(FSQueueBaseTestCase):

    def setUp(self):
        # pdb.set_trace()
        self.dbdir = os.path.join(testdir, "testqueue")
        self.q = bb.BasicBlackboardQueue(self.dbdir)

    def _newItem(self, name, data=None):
        return self.q.createItem(name, data)


class DataQueueTestCase(PolicyBBQueueTestCase):

    def setUp(self):
        # pdb.set_trace()
        self.dbdir = os.path.join(testdir, "testqueue")
        self.q = bb.DataQueue(self.dbdir)


class JobQueueTestCase(PolicyBBQueueTestCase):

    def setUp(self):
        # pdb.set_trace()
        self.dbdir = os.path.join(testdir, "testqueue")
        self.q = bb.JobQueue(self.dbdir)


__all__ = "AbsBBItemQTestCase FSQueueFailBaseTestCase FSQueueTestCase InMemoryBBQueueTestCase FSQueueBaseTestCase TransactionalBBQueueTestCase PolicyBBQueueTestCase DataQueueTestCase JobQueueTestCase ".split()

if __name__ == "__main__":
    unittest.main()
