"""
The basic blackboard API.  This file includes some abstract classes, including
BlackboardItem and BlackboardItemQueue.
"""
from __future__ import with_statement
import os, re

from lsst.pex.logging import Log
from lsst.pex.policy import Policy, PAFWriter
from lsst.utils.multithreading import SharedData

class BlackboardAccessError(Exception):
    """
    a generic error accessing the blackboard state
    """
    def __init__(self, msg=None, wrapped=None):
        """
        create the exception
        @param msg      the message describing the problem.  If not provided,
                            a default will be created.
        @param wrapped  an optional wrapped exception representing the
                            underlying reason for the failure.
        """
        if not msg:
            name= None
            if wrapped:
                msg = "Access error due to %s: %s" % \
                      (self._excname(wrapped), str(wrapped))
            else:
                msg = "Unknown error during blackboard access"
                
        Exception.__init__(self, msg)

        # the wrapped exception
        self.wrapped = None

    def _excname(self, excp):
        name = repr(excp.__class__)
        typematch = re.match(r"<type '([^']+)'>", name)
        if typematch:
            name = typematch.group(1)

        if name.startswith("exceptions."):
            name = name[11:]
        return name
            

class BlackboardUpdateError(BlackboardAccessError):
    """
    a failure was encountered while trying to update the state of a
    Blackboard queue.
    """
    def __init__(self, msg, wrapped):
        if not msg:
            name= None
            if wrapped:
                msg = "Update error due to %s: %s" % \
                      (self._excname(wrapped), str(wrapped))
            else:
                msg = "Unknown error during blackboard update"
                
        Exception.__init__(self, msg, wrapped)

class BlackboardPersistError(BlackboardUpdateError):
    """
    a failure was encountered while trying to persist the state of a
    Blackboard queue to disk.
    """
    def __init__(self, msg, wrapped=None):
        if not msg:
            msg = "IO failure while updating Blackboard"
        Exception.__init__(self, msg, wrapped)

class _AbstractBase(object):
    
    def __init__(self, fromSubclass=False):
        """create the base"""

        # confirm with caller this we are not instantiating this "abstract"
        # class directly
        _checkAbstract(fromSubclass, "BlackboardItemQueue")

    def _checkAbstract(self, fromSubclass, clsname):
        if not fromSubclass:
            raise RuntimeError('Programmer error: Apparent instantiation of "abstract" %s class' % clsname)

    def _notImplemented(self, methname):
        """
        raise a RuntimeError indicating that a non-implemented method was
        called.
        """
        raise RuntimeError("Programmer error: unimplemented method, %s, for class %s" % (methname, str(self.__class__)))


class BlackboardItem(_AbstractBase):
    """
    An abstract class representing an item in a blackboard queue
    containing a bunch of attributes.
    """

    def __init__(self, fromSubclass=False):
        """
        create an item with the given properties.
        @param properties    
        """
        self._checkAbstract(fromSubclass, "BlackboardItem")

    def getProperty(self, name, default=None):
        """
        return the value for a property
        @param name      the property name
        @parma default   the default value to return if the name is not set
        """
        self._notImplemented("getProperty")

    def getPropertyNames(self):
        """
        return the property names that make up this item
        """
        self._notImplemented("getPropertyNames")

    def __getitem__(self, name):
        return self.getProperty(name)

    def keys(self):
        return self.getPropertyNames()


class DictBlackboardItem(BlackboardItem):
    """
    An implementation of a BlackboardItem that stores properities via a
    simple dictionary
    """

    def __init__(self, properties=None):
        """
        create an item with the given properties.
        @param properties    A dictionary of properties
        """
        # the properties attached to this items
        self._props = {}
        if properties:
            self._props = properties.copy()

    def getProperty(self, name, default=None):
        """
        return the value for a property
        @param name      the property name
        @parma default   the default value to return if the name is not set
        """
        return self._props.get(name, default)

    def _setProperty(self, name, val):
        # set a property value
        self._props[name] = val

    def __getitem__(self, name):
        return self._props[name]

    def getPropertyNames(self):
        """
        return the property names that make up this item
        """
        return self._props.keys()

class PolicyBlackboardItem(BlackboardItem):
    """
    An implementation of a BlackboardItem that stores properities via a
    policy
    """

    def __init__(self, policyfile=None):
        """
        create an item with the given properties.
        @param properties    A policy
        """
        # the properties attached to this items
        self._props = None
        if policyfile:
            self._props = Policy.createPolicy(policyfile)
        else:
            self._props = Policy()

    def getProperty(self, name, default=None):
        """
        return the value for a property
        @param name      the property name
        @parma default   the default value to return if the name is not set
        """
        if not self._props.exists(name):
            return default
        elif self._props.isArray(name):
            return self._props.getArray(name)
        else:
            return self._props.get(name)

    def __getitem__(self, name):
        if not self._props.exists(name):
            raise KeyError(name)
        return BlackboardItem.__getitem__(self, name)

    def _setProperty(self, name, val):
        # set a property value
        if isinstance(val, list):
            self._props.set(name, val.pop(0))
            for v in val:
                self._props.add(name, v)
        else:
            self._props.set(name, val)

    def getPropertyNames(self):
        """
        return the property names that make up this item
        """
        return self._props.names()

    def _copyFrom(self, item):
        for name in item.getPropertyNames():
            self._setProperty(name, item.getProperty(name))

    @staticmethod
    def createFormatter():
        return PolicyBlackboardItem._Fmtr()

    class _Fmtr(object):
        def write(self, filename, item):
            pol = None
            if isinstance(item, PolicyBlackboardItem):
                pol = item._props
            else:
                delegate = PolicyBlackboardItem()
                delegate.copyFrom(item)
                pol = delegate._props

            writer = PAFWriter(filename)
            try:
                writer.write(pol, True)
            finally:
                writer.close()

        def openItem(self, filename):
            out = PolicyBlackboardItem()
            out._props = Policy.createPolicy(filename)
            return out

class ImplBlackboardItem(BlackboardItem):
    """
    An BlackboardItem that uses a delegate implementation to store properties 
    """

    def __init__(self, item):
        """
        wrap an BlackboardItem implementation
        """
        self._impl = item

    def getProperty(self, name, default=None):
        """
        return the value for a property
        @param name      the property name
        @parma default   the default value to return if the name is not set
        """
        return self._impl.getProperty(name, default)

    def _setProperty(self, name, val):
        # set a property value
        self._impl._setProperty(name, val)

    def getPropertyNames(self):
        """
        return the property names that make up this item
        """
        return self._impl.getPropertyNames()

    def __getitem__(self, name):
        return self._impl[name]

    

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
        self._notImplemented("isEmpty")

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

    def transferNextTo(self, queue, priority):
        """
        remove the item at the front of the queue and insert it into
        another queue with a given priority.  This funciton may be implemented
        to incorporate certain efficiencies to ensure a robust, atomic
        transfer.
        @param queue     the queue to transfer the item to
        @param priority  a measure of the priority for this item.  This value
        may be used to determine the proper position in the destination queue.
        """
        queue.insert(self.pop(), priority)

    def iterate(self):
        """
        return an iterator to the items in this queue.  
        """
        self._notImplemented("iterate")
        

class PersistingBlackboardItemQueue(object):
    """
    an abstract class representing a BlackboardItemQueue whose data is
    persisted to disk.

    The subclass should provide a value for self._mirror in its constructor
    to handle persistence to disk.  
    """

    def __init__(self, fromSubclass=False):
        """create an empty queue"""
        # confirm with caller this we are not instantiating this "abstract"
        # class directly
        _checkAbstract(fromSubclass, "PersistingBlackboardItemQueue")
        BlackboardItemQueue.__init__(self, True)

        # the queue itself
        self._items = []

        # a BlackboardItemQueue representing the mirror of the state on disk
        self._mirror = None

    def length(self):
        """
        return the number of items in this queue
        """
        return len(self._items)

    def __len__(self):
        return self.length()

    def isEmpty(self):
        """
        return True if the queue contains no items
        """
        return self.length() == 0

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
        self._mirror.pop(index)
        return self._items.pop(index)

    def append(self, item):
        """
        add the given BlackboardItem to the end of the queue.
        @param item    the BlackboardItem to add
        @throws BlackboardUpdateError  if the update fails
        """
        self._mirror.append(item)
        self._items.append(item)

    def insertAt(self, item, index=0):
        """
        insert a BlackboardItem at a given position in the queue.  If that
        position is out of range (including negative), it will be appended.  

        @param item   the Blackboard item to add.
        @param index  the position to insert the item at; default=0.
        """
        if index > len(self._items):
            return self.append(item)
        if index < 0:
            index = 0
        self._mirror.insertAt(item, index)
        self._items(index, item)

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

    def iterate(self):
        """
        return an iterator to the items in this queue.  
        """
        return list(self._items)

class _FSDBBlackboardQueue(BlackboardItemQueue):
    """
    a blackboard queue implementation that stores all its items as files on
    disk.

    This class is intended to be used internally to a
    PersistingBlackboardItemQueue instance.  
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
          raise BlackboardAccessError("Persistence directory does not exist: "+
                                      self._dbdir)

        # the file that keeps the order
        self._orderfile = self.orderFilename

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
            return filter(lambda f: not fsel.match(f),
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
                          "Queue order file missing items; adding: "+
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
            raise BlackboardPersistError("IOError getting item order: "+
                                         str(ex), ex)
        
    def pendingAddFor(self, file):
        """
        return the temporary file name that should used for a file as it's
        being written
        """
        return self._pendingFor(file, "add")

    def _pendingFor(self, file, prefix):
        out = ".%s.%s" % (prefix, file)
        i = 0
        while os.path.exists(out):
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

    def get(self, index=0):
        """
        return the n-th item in the queue (without removing it)
        @param index   the zero-based position index for the n-th item.  The
                         default is to get the next (first) item in the queue.
        @throws IndexError   if an item does not appear in this position 
        """
        return _Item(self._sd.files[index], self._formatter)

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
            
            if os.exists(os.path.join(self._dbdir, deleted)):
                raise BlackboardPersistError("concurrancy collision during " +
                                             "update:" + deleted)
            
            os.rename(file, deleted)
            return _Item(deleted, self._formatter, True)

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
            except BlackboardAccessError:
                # role back changes
                if os.path.exists(pending):
                    os.remove(pending)
                raise

    def _writeItem(self, item, path):
        # write the item file with a given name, returning the file's full path
        # Acquire self._sd before calling.

        try:
            self._formatter.write(path, item)
        except IOError, ex:
            # role back changes
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
            # role back changes
            self._sd.files.remove(-1)
            raise
        except OSError:
            # role back changes
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
                # role back changes
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

    def transferNextTo(self, queue, priority):
        """
        remove the item at the front of the queue and insert it into
        another queue with a given priority.  
        @param queue     the queue to transfer the item to
        @param priority  a measure of the priority for this item.  This value
        may be used to determine the proper position in the destination queue.
        """
        with self._sd:
            if self.isEmpty():
                raise EmptyBlackboardQueue()
        
            nextfile = self._sd.files[0]
            item = self.pop(0)
            try:
                queue.insert(item, priority)
            except BlackboardUpdateError, ex:
                # role back changes
                try:
                    self._insertItemFileAt(item.filename, nextfile, 0)
                except BlackboardAccessError, rbex:
                    if self._log and self._log.sends(Log.FAIL):
                        self._log.log(Log.FAIL,
                              "Rollback failure overrides original error: " +
                                      str(ex))
                    raise RuntimeError("Rollback failure: " + str(rbex))
                raise

    class _Item(BlackboardItem):

        def __init__(self, filename, formatter, purgeOnDelete=False):
            self.filename = filename
            self.purge = purgeOnDelete
            self._delegate = self.formatter.openItem(filename)

        def getProperty(self, name, default=None):
            return self._delegate.getProperty(name, default)

        def getPropertyNames(self):
            return self._delegate.getPropertyNames()

        def __del__(self):
            if self.purge and os.path.exists(self.filename):
                os.remove(self.filename)
    
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


