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
The basic blackboard API.  This file includes some abstract classes, including
BlackboardItem and BlackboardItemQueue.
"""
from __future__ import with_statement
import os
import re

from lsst.ctrl.sched.base import _AbstractBase
from exceptions import *
from item import *

from lsst.pex.logging import Log
from lsst.pex.policy import Policy, PAFWriter
from lsst.utils.multithreading import SharedData, LockProtected


class BlackboardItemQueue(_AbstractBase):
    """
    an abstract class representing an ordered list of blackboard items.
    """

    def __init__(self, fromSubclass=False):
        """create an empty queue"""

        # confirm with caller this we are not instantiating this "abstract"
        # class directly
        self._checkAbstract(fromSubclass, "BlackboardItemQueue")

    def length(self):
        """
        return the number of items in this queue
        """
        self._notImplemented("length")

    def __len__(self):
        return self.length()

    def isEmpty(self):
        """
        return True if the queue contains no items
        """
        return self.length() == 0

    def index(self, item):
        """
        return the position index of the given item in the queue or raise a
        ValueError
        """
        self._notImplemented("index")

    def get(self, index=0):
        """
        return the n-th item in the queue (without removing it)
        @param index   the zero-based position index for the n-th item.  The
                         default is to get the next (first) item in the queue.
        @throws IndexError   if an item does not appear in this position 
        """
        self._notImplemented("get")

    def pop(self, index=0):
        """
        remove and return the n-th item in the queue.
        @param index   the zero-based position index for the n-th item.  The
                         default is to pop the next (first) item in the queue.
        """
        self._notImplemented("pop")

    def append(self, item):
        """
        add the given BlackboardItem to the end of the queue.
        @param item    the BlackboardItem to add
        """
        self._notImplemented("append")

    def insertAt(self, item, index=0):
        """
        insert a BlackboardItem at a given position in the queue.  If that
        position is out of range (including negative), it will be appended.  

        @param item   the Blackboard item to add.
        @param index  the position to insert the item at; default=0.
        """
        self._notImplemented("insertAt")

    def insert(self, item, priority=0):
        """
        insert an item relative to the other items in the queue according
        to a priority measure.  The implementation will have its own
        algorithm for determining the best position.
        @param item      the Blackboard item to add.
        @param priority  a measure of the priority for this item where
                           higher numbers mean a higher priority.  This
                           value may be ignored by the implementation.
        """
        self._notImplemented("insert")

    def removeAll(self):
        """
        empty the queue of its contents.
        """
        self._notImplemented("removeAll")

    def transferNextTo(self, queue, priority=0):
        """
        remove the item at the front of the queue and insert it into
        another queue with a given priority.  This funciton may be implemented
        to incorporate certain efficiencies to ensure a robust, atomic
        transfer.
        @param queue     the queue to transfer the item to
        @param priority  a measure of the priority for this item.  This value
                            may be used to determine the proper position in
                            the destination queue.
        @return BlackboardQueueItem     the item that was moved
        """
        if self.isEmpty():
            raise EmptyQueueError()
        item = self.pop()
        queue.insert(item, priority)
        return item

    def iterate(self):
        """
        return an iterator to the items in this queue.  
        """
        self._notImplemented("iterate")


class PersistingBlackboardItemQueue(BlackboardItemQueue, LockProtected):
    """
    an abstract class representing a BlackboardItemQueue whose data is
    persisted to disk.

    The subclass should provide a value for self._mirror in its constructor
    to handle persistence to disk.  
    """
    # DEPRECATED:  remove this after integration is complete

    def __init__(self, lock=None, fromSubclass=False):
        """create an empty queue"""
        # confirm with caller this we are not instantiating this "abstract"
        # class directly
        self._checkAbstract(fromSubclass, "PersistingBlackboardItemQueue")
        BlackboardItemQueue.__init__(self, True)

        # we will make certain methods thread-safe
        LockProtected.__init__(self, lock)

        # the queue itself
        self._items = []

        # a BlackboardItemQueue representing the mirror of the state on disk
        self._mirror = None

    def length(self):
        """
        return the number of items in this queue
        """
        return len(self._items)

    def isEmpty(self):
        """
        return True if the queue contains no items
        """
        return self.length() == 0

    def index(self, item):
        """
        return the position index of the given item in the queue or raise a
        ValueError
        """
        return self._items.index(item)

    def get(self, index=0):
        """
        return the n-th item in the queue (without removing it)
        @param index   the zero-based position index for the n-th item.  The
                         default is to get the next (first) item in the queue.
        """
        return self._items[index]

    def pop(self, index=0):
        """
        remove and return the n-th item in the queue.
        @param index   the zero-based position index for the n-th item.  The
                         default is to pop the next (first) item in the queue.
        @throws BlackboardUpdateError  if the update fails
        """
        self._checkLocked()
        self._mirror.pop(index)
        return self._items.pop(index)

    def append(self, item):
        """
        add the given BlackboardItem to the end of the queue.
        @param item    the BlackboardItem to add
        @throws BlackboardUpdateError  if the update fails
        """
        self._checkLocked()
        self._mirror.append(item)
        self._items.append(item)

    def insertAt(self, item, index=0):
        """
        insert a BlackboardItem at a given position in the queue.  If that
        position is out of range (including negative), it will be appended.  

        @param item   the Blackboard item to add.
        @param index  the position to insert the item at; default=0.
        """
        self._checkLocked()
        if index < 0 or index > len(self._items):
            return self.append(item)

        self._mirror.insertAt(item, index)
        self._items.insert(index, item)

    def insert(self, item, priority=0):
        """
        insert an item relative to the other items in the queue according
        to a priority measure.  The implementation will have its own
        algorithm for determining the best position.  The default
        implimentation simply appends the item (ignoring priority). 
        @param item      the Blackboard item to add.
        @param priority  a measure of the priority for this item where
                           higher numbers mean a higher priority.  This
                           value may be ignored by the implementation.
        """
        return self.append(item)

    def removeAll(self):
        """
        empty the queue of its contents.
        """
        self._items = []
        self._mirror.removeAll()

    def iterate(self):
        """
        return an iterator to the items in this queue.  
        """
        self._checkLocked()
        return list(self._items)


class _FSDBBlackboardQueue(BlackboardItemQueue):
    """
    a blackboard queue implementation that stores all its items as files on
    disk.

    FSDB stands for "filesystem database".  This class is intended to be used 
    internally to a PersistingBlackboardItemQueue instance.  Subclasses 
    provide the actual format for the file on disk. 
    """
    # the regular expression used to filter out non-item files
    _fsel = re.compile("^[\._]")

    # the default name for the file giving the proper order of the items
    # (by their filenames)
    orderFilename = "_order.list"

    def __init__(self, dbdir, formatter, logger=None):
        """
        a directory containing the queue items
        """
        BlackboardItemQueue.__init__(self, True)

        # the logger to send messages to (if provided)
        self._log = logger

        # the parent directory where items are stored
        self._dbdir = dbdir
        parent = os.path.dirname(self._dbdir)
        if not os.path.isdir(parent) or not os.path.exists(parent):
            raise BlackboardPersistError("Unable to create queue directory: %s: directory not found" % parent)
        if not os.path.exists(self._dbdir):
            os.mkdir(self._dbdir)
        elif not os.path.isdir(self._dbdir):
            raise BlackboardAccessError("Queue directory: %s: not a directory"
                                        % self._dbdir)

        if not os.path.isdir(self._dbdir) or not os.path.exists(self._dbdir):
            raise BlackboardAccessError("Persistence directory does not exist: " +
                                        self._dbdir)

        # the file that keeps the order
        self._orderfile = os.path.join(self._dbdir, self.orderFilename)

        # the object that handles reading and writing item files
        self._formatter = formatter

        # setup up a lockable data object
        self._sd = SharedData(False, {"files": []})

        # a cache of the ordered list of files
        with self._sd:
            self._sd.files = self._loadOrder()
            self._syncOrder()

    def _loadOrder(self):
        # load the list of filenames in order from the order file on disk
        # Acquire self._sd before calling.

        out = []
        if not os.path.exists(self._orderfile):
            out = self._list()
            out.sort()
            return out

        try:
            with open(self._orderfile) as ordfile:

                out = " ".join(map(lambda l: l.strip(),
                                   ordfile.readlines())).split()

        except IOError, ex:
            raise BlackboardAccessError("IOError getting item order: " +
                                        str(ex), ex)

        return out

    def _list(self):
        # return the unordered list of files representing queue items.
        # Acquire self._sd before calling.

        try:
            return filter(lambda f: not self._fsel.match(f),
                          os.listdir(self._dbdir))
        except IOError, ex:
            raise BlackboardAccessError("IOError opening queue: " +
                                        str(ex), ex)

    def _syncOrder(self):
        # make sure current order list is in sync with the item files actually
        # on disk.  If not, write out a corrected order file
        # Acquire self._sd before calling.

        itemsFound = set(self._list())
        itemsRecorded = set(self._sd.files)
        updateNeeded = False

        # find the filenames that are on disk but not in the order list
        missing = itemsRecorded - itemsFound
        if len(missing) > 0:
            if self._log and self._log.sends(Log.WARN):
                self._log(Log.WARN,
                          "Queue order file missing items; adding: " +
                          " ".join(missing))
            add = list(missing)
            add.sort()
            self._sd.files.extend(add)
            updateNeeded = True

        # find the filenames that are in the order list but not on disk
        lost = itemsFound - itemsRecorded
        if len(lost) > 0:
            if self._log and self._log.sends(Log.WARN):
                self._log(Log.WARN,
                          "Queue order file contains missgin items; removing: "
                          + " ".join(lost))
            for item in lost:
                while item in self._sd.files:
                    self._sd.files.remove(item)
            updateNeeded = True

        # update the order file
        self._cacheOrder()

    def _cacheOrder(self):
        # dump the file order to the order file
        try:
            with open(self._orderfile, "w") as ordfile:

                for item in self._sd.files:
                    print >> ordfile, item

        except IOError, ex:
            raise BlackboardPersistError("IOError getting item order: " +
                                         str(ex), ex)

    def filenameFor(self, item):
        """
        return filename for persisting the given item.  This name is usually
        constructed from the contents of the item.  
        """
        name = item.getProperty(Props.NAME, "unknown")
        ext = self._formatter.filenameExt()

        out = "%s.%s" % (name, ext)
        i = 0
        while os.path.exists(os.path.join(self._dbdir, out)):
            i += 1
            out = "%s.%i.%s" % (name, i, ext)
        return out

    def pendingAddFor(self, file):
        """
        return the temporary file name that should used for a file as it's
        being written
        """
        return self._pendingFor(file, "add")

    def _pendingFor(self, file, prefix):
        out = ".%s.%s" % (prefix, file)
        i = 0
        while os.path.exists(os.path.join(self._dbdir, out)):
            i += 1
            out = ".%s.%i.%s" % (prefix, i, file)
        return out

    def pendingDelFor(self, file):
        """
        return the temporary file name that should used for a file as it's
        being written
        """
        return self._pendingFor(file, "del")

    def length(self):
        """
        return the number of items in this queue
        """
        return len(self._sd.files)

    def isEmpty(self):
        """
        return True if the queue contains no items
        """
        return self.length() == 0

    def index(self, item):
        """
        return the position index of the given item in the queue or raise a
        ValueError.  This implementation is slow as it potentially must open
        multiple files and do a content comparison with the given item.  It
        should not be called as part of normal us of this module.

        This implementation actually throws an exception as a programmer
        guard.
        """
        raise RuntimeError("Programmer Error: should not call index() on filesytem-based implementation")

    def get(self, index=0):
        """
        return the n-th item in the queue (without removing it)
        @param index   the zero-based position index for the n-th item.  The
                         default is to get the next (first) item in the queue.
        @throws IndexError   if an item does not appear in this position 
        """
        return self._Item(os.path.join(self._dbdir, self._sd.files[index]),
                          self._formatter)

    def pop(self, index=0):
        """
        remove and return the n-th item in the queue.
        @param index   the zero-based position index for the n-th item.  The
                         default is to pop the next (first) item in the queue.
        """
        with self._sd:
            file = self._sd.files[index]
            deleted = ".del." + file

            # prepend the dbdir
            (file, deleted) = map(lambda f: os.path.join(self._dbdir, f),
                                  [file, deleted])

            if os.path.exists(os.path.join(self._dbdir, deleted)):
                raise BlackboardPersistError("concurrancy collision during " +
                                             "update:" + deleted)

            # move the file to a name pending delete
            os.rename(file, deleted)

            # remove from the file order
            self._sd.files.pop(index)
            try:
                self._cacheOrder()
            except Exception, ex:
                # roll back!
                try:
                    os.rename(deleted, file)
                except Exception, rbex:
                    self._logRollbackFail(ex, rbex)
                    raise BlackboardRollbackError(ex, rbex)
                raise

            # deleted will get deleted when the item is destroyed
            return self._Item(deleted, self._formatter, True)

    def append(self, item):
        """
        add the given BlackboardItem to the end of the queue.

        This implimentation attempts to be atomic: if any failure occurs,
        the item is not appended and the integrity of this object's state
        is preserved.

        @param item    the BlackboardItem to add
        """
        with self._sd:
            file = self.filenameFor(item)
            pending = os.path.join(self._dbdir, self.pendingAddFor(file))
            self._writeItem(item, pending)

            try:
                self._insertItemFileAt(pending, file, -1)
            except BlackboardAccessError, ex:
                # roll back changes
                try:
                    if os.path.exists(pending):
                        os.remove(pending)
                except Exception, rbex:
                    self._logRollbackFail(ex, rbex)
                    raise BlackboardRollbackError(ex, rbex)
                raise

    def _writeItem(self, item, path):
        # write the item file with a given name, returning the file's full path
        # Acquire self._sd before calling.

        try:
            self._formatter.write(path, item)
        except IOError, ex:
            # roll back changes
            if os.path.exists(path):
                os.remove(path)
            raise BlackboardPersistError("IOError: " + str(ex), ex)

        return file

    def _insertItemFileAt(self, src, dest, index):
        # take an item already written to a file and insert it into this
        # queue.  This is done by moving it from src to dest and inserting
        # its name into the order list at the given position.  src should
        # be the full path, but dest should not be.  If index is out of
        # range, the item will be appended

        try:
            if index < 0 or index > len(self._sd.files):
                index = -1
                self._sd.files.append(dest)
            else:
                self._sd.files.insert(index, dest)
            self._cacheOrder()

            os.rename(src, os.path.join(self._dbdir, dest))
        except BlackboardAccessError:
            # roll back changes
            self._sd.files.remove(-1)
            raise
        except OSError:
            # roll back changes
            self._sd.files.remove(-1)
            self._cacheOrder()
            raise

    def insertAt(self, item, index=0):
        """
        insert a BlackboardItem at a given position in the queue.  If that
        position is out of range (including negative), it will be appended.  

        @param item   the Blackboard item to add.
        @param index  the position to insert the item at; default=0.
        """
        with self._sd:
            file = self.filenameFor(item)
            pending = os.path.join(self._dbdir, self.pendingAddFor(file))
            self._writeItem(item, pending)

            try:
                self._insertItemFileAt(pending, file, index)
            except BlackboardAccessError:
                # roll back changes
                if os.path.exists(pending):
                    os.remove(pending)
                raise

    def insert(self, item, priority=0):
        """
        insert an item relative to the other items in the queue according
        to a priority measure.  The implementation will have its own
        algorithm for determining the best position.

        This implementation simply appends the item

        @param item      the Blackboard item to add.
        @param priority  a measure of the priority for this item where
                           higher numbers mean a higher priority.  This
                           value may be ignored by the implementation.
        """
        self.append(item)

    def removeAll(self):
        """
        empty the queue of its contents.
        """
        with self._sd:
            for file in self._list():
                os.remove(os.path.join(self._dbdir, file))
            if os.path.exists(self._orderfile):
                os.remove(self._orderfile)
            self._sd.files = []

    def transferNextTo(self, queue, priority=0):
        """
        remove the item at the front of the queue and insert it into
        another queue with a given priority.  
        @param queue     the queue to transfer the item to
        @param priority  a measure of the priority for this item.  This value
                            may be used to determine the proper position in
                            the destination queue.
        """
        with self._sd:
            if self.isEmpty():
                raise EmptyQueueError()

            nextfile = self._sd.files[0]
            item = self.pop(0)
            try:
                queue.insert(item, priority)
            except BlackboardUpdateError, ex:
                # roll back changes
                try:
                    self._insertItemFileAt(item.filename, nextfile, 0)
                except BlackboardAccessError, rbex:
                    self._logRollbackFail(ex, rbex)
                    raise BlackboardRollbackError(ex, rbex)
                raise

    def _logRollbackFail(self, origex, rbex):
        if self._log and self._log.sends(Log.WARN):
            self._log.log(Log.FAIL, "Failure during rollback: " + str(rbex))
            self._log.log(Log.WARN,
                          "Rollback failure overriding original error: " +
                          str(origex))

    class _Item(BlackboardItem):

        def __init__(self, filename, formatter, purgeOnDelete=False):
            self.filename = filename
            self.purge = purgeOnDelete
            self._delegate = formatter.openItem(filename)

        def getProperty(self, name, default=None):
            return self._delegate.getProperty(name, default)

        def getPropertyNames(self):
            return self._delegate.getPropertyNames()

        def _setProperty(self, name, val=None):
            return self._delegate._setProperty(name, val)

        def hasProperty(self, name):
            return self._delegate.hasProperty(name)

        def __del__(self):
            if self.purge and os.path.exists(self.filename):
                os.remove(self.filename)

    def iterate(self):
        """
        return an iterator to the items in this queue.  
        """
        for i in xrange(self.length()):
            yield self.get(i)


class _PolicyBlackboardQueue(_FSDBBlackboardQueue):
    """
    an filesystem-based queue that stores items as Policy files
    """

    def __init__(self, dbdir, logger=None):
        """
        create an empty queue using the given queue directory
        """
        fmtr = PolicyBlackboardItem.createFormatter()
        _FSDBBlackboardQueue.__init__(self, dbdir, fmtr, logger)


class InMemoryBlackboardQueue(BlackboardItemQueue):
    """
    Simple implementation of a queue where the data is kept completely in
    memory.  No lock-based protection is provide in this implementation.
    """

    def __init__(self):
        """create an empty queue"""
        BlackboardItemQueue.__init__(self, True)

        # the queue itself
        self._items = []

    def length(self):
        """
        return the number of items in this queue
        """
        return len(self._items)

    def isEmpty(self):
        """
        return True if the queue contains no items
        """
        return self.length() == 0

    def index(self, item):
        """
        return the position index of the given item in the queue or raise a
        ValueError.  Note that this implementation does not match items by
        value but by reference.
        """
        return self._items.index(item)

    def get(self, index=0):
        """
        return the n-th item in the queue (without removing it)
        @param index   the zero-based position index for the n-th item.  The
                         default is to get the next (first) item in the queue.
        """
        try:
            return self._items[index]
        except IndexError:
            raise IndexError("queue index out of range: %i" % index)

    def pop(self, index=0):
        """
        remove and return the n-th item in the queue.
        @param index   the zero-based position index for the n-th item.  The
                         default is to pop the next (first) item in the queue.
        @throws BlackboardUpdateError  if the update fails
        """
        return self._items.pop(index)

    def append(self, item):
        """
        add the given BlackboardItem to the end of the queue.
        @param item    the BlackboardItem to add
        @throws BlackboardUpdateError  if the update fails
        """
        self._items.append(item)

    def insertAt(self, item, index=0):
        """
        insert a BlackboardItem at a given position in the queue.  If that
        position is out of range (including negative), it will be appended.  

        @param item   the Blackboard item to add.
        @param index  the position to insert the item at; default=0.
        """
        if index < 0 or index > len(self._items):
            return self.append(item)

        self._items.insert(index, item)

    def insert(self, item, priority=0):
        """
        insert an item relative to the other items in the queue according
        to a priority measure.  The implementation will have its own
        algorithm for determining the best position.  The default
        implimentation simply appends the item (ignoring priority). 
        @param item      the Blackboard item to add.
        @param priority  a measure of the priority for this item where
                           higher numbers mean a higher priority.  This
                           value may be ignored by the implementation.
        """
        return self.append(item)

    def removeAll(self):
        """
        empty the queue of its contents.
        """
        self._items = []

    def iterate(self):
        """
        return an iterator to the items in this queue.  
        """
        return list(self._items)


class TransactionalBlackboardQueue(BlackboardItemQueue, LockProtected):
    """
    a queue that supports grouping of actions into transactions that must
    all succeed or else the state is rolled back to its pre-transaction state.
    """

    def __init__(self, persistImpl, logger=None, lock=SharedData()):
        """
        create an empty queue
        @param logger  a logger to use for warnings and failures
        @param lock    a lock to use for locking multiple queues together.
        """
        # confirm with caller this we are not instantiating this "abstract"
        # class directly
        BlackboardItemQueue.__init__(self, True)

        # we will make certain methods thread-safe
        LockProtected.__init__(self, lock)

        # the logger to use
        self._log = logger

        # the in-memory copy of the queue
        self._memq = InMemoryBlackboardQueue()

        # the disk-persisted copy of the queue
        self._dskq = persistImpl

        # load the disk queue into memory
        self._syncWithDisk()

        # the list of actions in the current transaction
        self._pending = None

        # the "roll-back queue" representing the pre-transaction state
        self._rbq = None

    def __enter__(self):
        """
        start a transaction
        """
        LockProtected.__enter__(self)

        if self._rbq is None:
            # create a copy of the pre-transations state
            self._rbq = InMemoryBlackboardQueue()
            for item in self._memq.iterate():
                self._rbq.append(item)

        if self._pending is None:
            self._pending = []

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if self._rbq is not None:
                if exc_type:
                    # non-commit failure occurred; roll back memq
                    if self._pending:
                        self._memq = self._rbq
                    # self._rollback(self._memq, exc_value)

                else:
                    # commit changes...
                    try:
                        # ... to disk
                        self._applyPending(self._dskq)
                    except Exception, ex:
                        # commit failure occurred; roll back both queues
                        self._rollback(self._dskq, ex)
                        self._memq = self._rbq
                        # self._rollback(self._memq, ex)
                        raise

        finally:
            self._rbq = None
            self._pending = None
            out = LockProtected.__exit__(self, exc_type, exc_value, traceback)

        return out

    class _Action(object):

        def __init__(self, func, kw):
            self._f = func
            self._kw = kw

        def execute(self, queue):
            f = getattr(queue, self._f)
            return f(**self._kw)

    def _applyPending(self, queue):
        if self._pending:
            for action in self._pending:
                action.execute(queue)

    def _syncWithMemory(self):
        self._sync(self._memq, self._dskq)

    def _syncWithDisk(self):
        self._sync(self._dskq, self._memq)

    def _rollback(self, queue, exc=None):
        try:
            self._sync(self._rbq, queue)
        except Exception, ex:
            if exc and self._log:
                self._log.log(Log.FAIL, "roll back failure hiding original error: %s" % exc)
            raise BlackboardRollbackError(exc, ex,
                                          "Rollback error leaving corrupt queue: " +
                                          str(ex))

    def _sync(self, fromq, toq):
        toq.removeAll()
        for item in fromq.iterate():
            toq.append(item)

    def length(self):
        """
        return the number of items in this queue
        """
        return self._memq.length()

    def index(self, item):
        """
        return the position index of the given item in the queue or raise a
        ValueError
        """
        return self._memq.index(item)

    def get(self, index=0):
        """
        return the n-th item in the queue (without removing it)
        @param index   the zero-based position index for the n-th item.  The
                         default is to get the next (first) item in the queue.
        @throws IndexError   if an item does not appear in this position 
        """
        return self._memq.get(index)

    def pop(self, index=0):
        """
        remove and return the n-th item in the queue.
        @param index   the zero-based position index for the n-th item.  The
                         default is to pop the next (first) item in the queue.
        """
        with self._lp_lock:
            if self._pending is None:
                # commit right away
                try:
                    self._dskq.pop(index)
                except Exception, ex:
                    with self:
                        self._rollback(self._dskq, ex)
                    raise

            else:
                # add to pending
                self._pending.append(self._Action("pop", {"index": index}))

            return self._memq.pop(index)

    def append(self, item):
        """
        add the given BlackboardItem to the end of the queue.
        @param item    the BlackboardItem to add
        """
        with self._lp_lock:
            if self._pending is None:
                # commit right away
                try:
                    self._dskq.append(item)
                except Exception, ex:
                    with self:
                        self._rollback(self._dskq, ex)
                    raise
            else:
                # add to pending
                self._pending.append(self._Action("append", {"item": item}))

            self._memq.append(item)

    def insertAt(self, item, index=0):
        """
        insert a BlackboardItem at a given position in the queue.  If that
        position is out of range (including negative), it will be appended.  

        @param item   the Blackboard item to add.
        @param index  the position to insert the item at; default=0.
        """
        with self._lp_lock:
            if self._pending is None:
                # commit right away
                try:
                    self._dskq.insertAt(item, index)
                except Exception, ex:
                    with self:
                        self._rollback(self._dskq, ex)
                    raise
            else:
                # add to pending
                self._pending.append(self._Action("insertAt",
                                                  {"item": item,
                                                   "index": index}))

            self._memq.insertAt(item, index)

    def insert(self, item, priority=0):
        """
        insert an item relative to the other items in the queue according
        to a priority measure.  This implementation simply appends the item
        to the end of the queue
        @param item      the Blackboard item to add.
        @param priority  a measure of the priority for this item where
                           higher numbers mean a higher priority.  This
                           value may be ignored by the implementation.
        """
        self.append(item)

    def removeAll(self):
        """
        empty the queue of its contents.
        """
        with self._lp_lock:
            if self._pending is None:
                # commit right away
                try:
                    self._dskq.removeAll()
                except Exception, ex:
                    with self:
                        self._rollback(self._dskq, ex)
                    raise

            else:
                # add to pending
                self._pending.append(self._Action("removeAll", {}))

            self._memq.removeAll()

    def transferNextTo(self, queue, priority=0):
        """
        remove the item at the front of the queue and insert it into
        another queue with a given priority.  This funciton may be implemented
        to incorporate certain efficiencies to ensure a robust, atomic
        transfer.
        @param queue     the queue to transfer the item to
        @param priority  a measure of the priority for this item.  This value
                            may be used to determine the proper position in
                            the destination queue.
        """
        if self.isEmpty():
            raise EmptyQueueError()
        if isinstance(queue, LockProtected):
            with queue:
                queue.insert(self.pop(), priority)
        else:
            queue.insert(self.pop(), priority)

    def iterate(self):
        """
        return an iterator to the items in this queue.  
        """
        return self._memq.iterate()


class BasicBlackboardQueue(TransactionalBlackboardQueue):
    """
    a basic queue that persists its data to disk
    """

    def __init__(self, dbdir, logger=None, lock=SharedData()):
        """
        create an empty queue
        @param dbdir   the directory to persist the queue items to
        @param logger  a logger to use for warnings and failures
        @param lock    a lock to use for locking multiple queues together.
        """
        persistq = _PolicyBlackboardQueue(dbdir, logger)
        TransactionalBlackboardQueue.__init__(self, persistq, logger, lock)

    def _syncWithDisk(self):
        """
        match in memory contents with that of the disk mirror.  
        """
        self._memq.removeAll()
        for item in self._dskq.iterate():
            self._memq.append(self._wrapItem(item))

    def _wrapItem(self, item):
        """
        wrap a persistable item for use in this queue.  A subclass should
        override this to provide the proper wrapping class.  
        """
        return BasicBlackboardItem(item)

    def createItem(self, name, data=None):
        """
        create an item for the blackboard
        """
        return BasicBlackboardItem.createItem(name, data)

    def createDataProductItem(self, name, type="", indexTypes=None,
                              success=True, props=None):
        """
        create a BlackboardItem with the given properties
        @param name        the item name
        @param type        the value for the TYPE property, indicating the type
                             of dataset being described.
        @param indexTypes  an array of names giving the names of the indexs
                             that identify the dataset.
        @param success     True if the dataset was indeed successfully create.
        @param props       a dictionary of additional properties
        """
        return DataProductItem(name, type, indexTypes, success, props)

    def createJobItem(self, name, files=None, props=None):
        """
        create a BlackboardItem with the given properties
        @param name    the item name
        @param props   a dictionary of properties
        """
        return JobItem(name, files, props)


class _DeprecatedBasicBlackboardQueue(PersistingBlackboardItemQueue):
    """
    a basic queue that persists its data to disk
    """
    # DEPRECATED:  remove this after integration is complete

    def __init__(self, dbdir, logger=None, lock=None):
        """
        create an empty queue
        @param dbdir   the directory to persist the queue items to
        @param logger  a logger to use for warnings and failures
        @param lock    a lock to use for locking multiple queues together.
        """
        PersistingBlackboardItemQueue.__init__(self, lock, True)
        self._mirror = _PolicyBlackboardQueue(dbdir, logger)
        self._syncWithMirror()

    def _syncWithMirror(self):
        """
        match in memory contents with that of the mirror.  This should be
        called by the subclass after the mirror queue is set.
        """
        self._items = []
        for item in self._mirror.iterate():
            self._items.append(self._wrapItem(item))

    def _wrapItem(self, item):
        """
        wrap a persistable item for use in this queue.  A subclass should
        override this to provide the proper wrapping class.  
        """
        return BasicBlackboardItem(item)

    def createItem(self, name, data=None):
        """
        create an item for the blackboard
        """
        return BasicBlackboardItem.createItem(name, data)

    def createDataProductItem(self, name, type="", indexTypes=None,
                              success=True, props=None):
        """
        create a BlackboardItem with the given properties
        @param name        the item name
        @param type        the value for the TYPE property, indicating the type
                             of dataset being described.
        @param indexTypes  an array of names giving the names of the indexs
                             that identify the dataset.
        @param success     True if the dataset was indeed successfully create.
        @param props       a dictionary of additional properties
        """
        return DataProductItem(name, type, indexTypes, success, props)

    def createJobItem(self, name, files=None, props=None):
        """
        create a BlackboardItem with the given properties
        @param name    the item name
        @param props   a dictionary of properties
        """
        return JobItem(name, files, props)


class DataQueue(BasicBlackboardQueue):
    """
    a Blackboard item queue that holds DataProduct items
    """

    def _wrapItem(self, item):
        """
        wrap a persistable item for use in this queue.
        """
        return DataProductItem(item)


class JobQueue(BasicBlackboardQueue):
    """
    a Blackboard item queue that holds DataProduct items
    """

    def _wrapItem(self, item):
        """
        wrap a persistable item for use in this queue.
        """
        return JobItem(item)


__all__ = "BlackboardItemQueue BasicBlackboardQueue DataQueue JobQueue".split()
