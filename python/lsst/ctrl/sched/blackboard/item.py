"""
The abstract BlackboardItem class and implementations
"""

from base import _AbstractBase

from lsst.pex.policy import Policy, PAFWriter
import pdb

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

    def hasProperty(self, name):
        """
        return True if the property with the given name is available
        """
        self._notImplemented("hasProperty")

    def __getitem__(self, name):
        return self.getProperty(name)

    def keys(self):
        return self.getPropertyNames()

    def has_key(self, name):
        return self.hasProperty(name)

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

    def hasProperty(self, name):
        """
        return True if the property with the given name is available
        """
        return self._props.has_key(name)

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
        @param policyfile    A policy
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

    def hasProperty(self, name):
        """
        return True if the property with the given name is available
        """
        return self._props.exists(name)

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
                delegate._copyFrom(item)
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

        def filenameExt(self):
            """
            return the recommended extension for the format this writes out
            """
            return "paf"

class ImplBlackboardItem(BlackboardItem):
    """
    An BlackboardItem that uses a delegate implementation to store properties.

    The purpose of class is to serve as a base class for role-based
    item classes while allowing the internal storage choice to be
    handled by the delegate.
    """

    def __init__(self, item):
        """
        wrap an BlackboardItem implementation
        @param item     the item that is actually storing the properties
        """
        self._impl = item

    def getProperty(self, name, default=None):
        """
        return the value for a property
        @param name      the property name
        @parma default   the default value to return if the name is not set
        """
        return self._impl.getProperty(name, default)

    def hasProperty(self, name):
        """
        return True if the property with the given name is available
        """
        return self._impl.hasProperty(name)

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

class BasicBlackboardItem(ImplBlackboardItem):
    """
    A simple, generic BlackboardItem.

    It supports the following common properties:
    @verbatim
    NAME       a name for the item.  There is no expectation that it is 
                 unique across items, but typically it is.
    @endverbatim

    These are normally created via createItem() which chooses the internal
    representation of the data.
    """

    NAME = "NAME"

    def __init__(self, impl, name=None):
        """
        create an item.  This is not usually called directly by the user but
        rather via createItem().
        @param impl     the item that is actually storing the properties
        @param name     the value for the NAME property.  If provided, this
                          override the NAME property in impl (if set).  If
                          not provided, this will default to an empty string.
        """
        ImplBlackboardItem.__init__(self, impl)
        if name is None and not impl.hasProperty(self.NAME):
            name = ""
        if name is not None:
            impl._setProperty(self.NAME, name)

    def getName(self):
        """
        return the item (dataset or job) name.  Equivalent to
        self.getProperty("NAME")
        """
        return self.getProperty(self.NAME)
        
    @staticmethod
    def createItem(name, props=None):
        """
        create a BlackboardItem with the given properties
        @param name    the item name
        @param props   a dictionary of properties
        """
        impl = PolicyBlackboardItem()
        if props:
            for key in props.keys():
                impl._setProperty(key, props[key])
        out = BasicBlackboardItem(impl, name)
        return out

class DataProductItem(BasicBlackboardItem):
    """
    An item representing a created data product.

    It supports the following common properties:
    @verbatim
    NAME       a name for the item.  There is no expectation that it is 
                 unique across items, but typically it is.
    SUCCESS    a boolean value indicating whether the product was
                 successfully created.  If False, an attempt to create the
                 product was made but it was unsuccessful.
    TYPE       a name indicating the type of product
    IDXTYPES   the names of indicies that characterize the 
    @endverbatim

    Generally, the item will also contain the index values (as strings)
    for each names given in IDXTYPES.
    """

    SUCCESS  = "SUCCESS"
    TYPE     = "TYPE"
    IDXTYPES = "IDXTYPES"
    
    def __init__(self, impl, name=None, type=None, indexTypes=None,
                 success=None):
        """
        create an item.  This is not usually called directly by the user but
        rather via createItem().  If the standard property parameters
        (name, type, indexTypes, and success) are provided, they will override
        the the corresponding properties in impl; if not provided, the default
        is set.
        @param impl     the item that is actually storing the properties
        @param name     the value for the NAME property (Default: empty string)
        @param type     the value for the TYPE property, indicating the type
                          of dataset being described (Default: empty string).
        @param indexTypes  an array of names giving the names of the indexs
                        that identify the dataset.
        @param success  True if the dataset was indeed successfully create.
                           (Default: True)
        """
        BasicBlackboardItem.__init__(self, impl, name)
        if type is None and not impl.hasProperty(self.TYPE):
            type = ""
        if type is not None:
            impl._setProperty(self.TYPE, type)
        if indexTypes:
            impl._setProperty(self.IDXTYPES, indexTypes)
        if success is None and not impl.hasProperty(self.SUCCESS):
            success = True
        if success is not None:
            impl._setProperty(self.SUCCESS, success)

    def getType(self):
        """
        return the dataset type.  Equivalent to self.getProperty("TYPE")
        """
        return self.getProperty(self.TYPE)
        
    def isSuccessful(self):
        """
        return the dataset type.  Equivalent to self.getProperty("SUCCESS")
        """
        return self.getProperty(self.SUCCESS)
        
    @staticmethod
    def createItem(name, type="", indexTypes=None, success=True, props=None):
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
        impl = PolicyBlackboardItem()
        if props:
            for key in props.keys():
                impl._setProperty(key, props[key])
        out = DataProductItem(impl, name, type, indexTypes, success)
        return out


class JobItem(BasicBlackboardItem):
    """
    A simple, generic BlackboardItem.

    It supports the following common properties:
    @verbatim
    NAME    a name for the item.  There is no expectation that it is 
              unique across items, but typically it is.
    FILES   a list of the files that serve as input to a pipeline
    @endverbatim

    These are normally created via createItem() which chooses the internal
    representation of the data.
    """

    FILES = "FILES"

    def __init__(self, impl, name=None, files=None):
        """
        create an item.  This is not usually called directly by the user but
        rather via createItem().  If the standard property parameters
        (name, type, indexTypes, and success) are provided, they will override
        the the corresponding properties in impl; if not provided, the default
        is set.
        @param impl     the item that is actually storing the properties
        @param name     the value for the NAME property.
        @param files    the list of input files required by this job.
        """
        BasicBlackboardItem.__init__(self, impl, name)
        if files:
            impl._setProperty(self.FILES, files)

        self.triggerHandler = None

    def getFiles(self):
        return self.getProperty(self.FILES)

    def setTriggerHandler(self, handler):
        """
        attach a trigger handler instance to this job.
        """
        self.triggerHandler = handler

    def setNeededDataset(self, dataset):
        """
        note that a dataset that is one of the required triggers for this
        job is available.
        @param dataset    the trigger dataset that is ready.
        @return bool   True if the dataset was needed but not added until now
        """
        if self.triggerHandler:
            return self.triggerHandler.addDataset(dataset)
        return False

    def isReady(self):
        """
        return True if all the trigger files have been produced and the job is
        ready to be scheduled to a pipeline.
        """
        return self.triggerHandler is not None and self.triggerHandler.isReady()
        

    @staticmethod
    def createItem(name, files=None, props=None):
        """
        create a BlackboardItem with the given properties
        @param name    the item name
        @param props   a dictionary of properties
        """
        impl = PolicyBlackboardItem()
        if props:
            for key in props.keys():
                impl._setProperty(key, props[key])
        out = JobItem(impl, name, files)
        return out

class Props(object):
    """
    an enumeration of standard BlackboardItem property names
    """
    NAME     = BasicBlackboardItem.NAME
    SUCCESS  =     DataProductItem.SUCCESS
    TYPE     =     DataProductItem.TYPE
    IDXTYPES =     DataProductItem.IDXTYPES
    FILES    =             JobItem.FILES


__all__ = "BlackboardItem DictBlackboardItem PolicyBlackboardItem ImplBlackboardItem BasicBlackboardItem DataProductItem JobItem Props".split()
