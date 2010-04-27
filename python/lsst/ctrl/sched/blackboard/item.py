"""
The abstract BlackboardItem class and implementations
"""

from lsst.ctrl.sched.base import _AbstractBase
from lsst.ctrl.sched import Dataset

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
    DATASET    a policy-serialization of a Dataset description
    @endverbatim
    """
    SUCCESS  = "SUCCESS"
    DATASET  = "DATASET"
    
    def __init__(self, impl, name=None, success=None, dataset=None):
                 
        """
        create an item.  This is not usually called directly by the user but
        rather via createItem().  If the standard property parameters
        (name, type, indexTypes, and success) are provided, they will override
        the the corresponding properties in impl; if not provided, the default
        is set.
        @param impl     the item that is actually storing the properties
        @param name     the value for the NAME property (Default: empty string)
        @param success  True if the dataset was indeed successfully create.
                           (Default: True)
        @param dataset  a full description of the dataset
        """
        BasicBlackboardItem.__init__(self, impl, name)
        if success is None and not impl.hasProperty(self.SUCCESS):
            success = True
        if success is not None:
            impl._setProperty(self.SUCCESS, success)

        if dataset:
            if success is None:
                impl._setProperty(self.SUCCESS, dataset.valid)
            if isinstance(dataset, Dataset):
                dataset = dataset.toPolicy()
            if not isinstance(dataset, Policy):
                raise ValueError("DataProductItem: input dataset not Dataset or Policy")
            if success is not None:
                dataset.set("valid", success)
            impl._setProperty(self.DATASET, dataset)

    def getDataset(self):
        """
        return a Dataset instance describing this product or None if a
        description is not available.
        """
        ds = self.getProperty(self.DATASET)
        if ds:
            ds = Dataset.fromPolicy(ds)
        return ds

    def getType(self):
        """
        return the dataset type or None if it isn't known
        """
        ds = self.getDataset()
        if ds:
            return ds.type
        return None
        
    def isSuccessful(self):
        """
        return the dataset type.  Equivalent to self.getProperty("SUCCESS")
        """
        return self.getProperty(self.SUCCESS)
        
    @staticmethod
    def createItem(dataset, success=True, props=None):
        """
        create a BlackboardItem for a given dataset
        @param dataset     a Dataset instance describing the data product
        @param success     True if the dataset was indeed successfully create.
        @param props       a dictionary of additional properties
        """
        impl = PolicyBlackboardItem()
        if props:
            for key in props.keys():
                impl._setProperty(key, props[key])

        name = dataset.toString()
        out = DataProductItem(impl, name, success, dataset)
        return out

class PipelineItem(BasicBlackboardItem):
    """
    a BlackboardItem representing pipeline that is ready to run a job.

    It supports the following common properties:
    @verbatim
    NAME        a name for the item.  There is no expectation that it is 
                  unique across items, but typically it is.
    ORIGINATOR  a unique identifier for the instance of the pipeline that
                  this item represents.  
    @endverbatim

    These are normally created via createItem() which chooses the internal
    representation of the data.
    """
    ORIGINATOR = "ORIGINATOR"
    RUNID = "RUNID"

    def __init__(self, impl, name, runid, pipelineId):
        """
        create the item
        @param impl         the item that is actually storing the properties
        @param name         the value for the NAME property.
        @param pipelineId   the unique Id for the pipeline
        """
        BasicBlackboardItem.__init__(self, impl, name)
        self._setProperty(self.ORIGINATOR, _encodeId(pipelineId))
        self._setProperty(self.RUNID, runid)

    def getOriginator(self):
        """
        return the identifier for the pipeline
        """
        return _decodeId(self.getProperty(self.ORIGINATOR))

    def getRunId(self):
        """
        return the run ID for the pipeline
        """
        return self.getProperty(self.RUNID)

    @staticmethod
    def createItem(name, runId, pipelineId, props=None):
        """
        create a BlackboardItem with the given properties
        @param name         the item name
        @param runId        the run ID that the pipeline is running under
        @param pipelineId   the unique ID for pipeline this represents
        @param props        a dictionary of properties
        """
        impl = PolicyBlackboardItem()
        if props:
            for key in props.keys():
                impl._setProperty(key, props[key])
        out = PipelineItem(impl, name, runId, pipelineId)
        return out

def _encodeId(id):
    prts = []
    prts.append(int((id >> 48) & 0xffff))
    prts.append(int((id >> 32) & 0xffff))
    prts.append(int((id >> 16) & 0xffff))
    prts.append(int(id & 0xffff))
    return prts
def _decodeId(quad):
    return long(((1L *quad[0]) << 48) | ((1L *quad[1]) << 32) |
                ((1L *quad[2]) << 16) | quad[3])

    

class JobItem(BasicBlackboardItem):
    """
    A BlackboardItem representing a job to be processed by a pipeline.

    It supports the following common properties:
    @verbatim
    NAME       a name for the item.  There is no expectation that it is 
                 unique across items, but typically it is.
    INPUT      a list of the datasets that serve as input a pipeline job
    OUTPUT     a list of the datasets will be produced by a pipeline job
    @endverbatim

    These are normally created via createItem() which chooses the internal
    representation of the data.
    """

    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    PIPELINEID = "PIPELINEID"
    JOBIDENTITY = "JOBIDENTITY"
    SUCCESS = "SUCCESS"

    def __init__(self, impl, jobDataset=None, name=None, inputs=None, 
                 outputs=None, triggerHandler=None):
        """
        create an item.  This is not usually called directly by the user but
        rather via createItem().  
        @param impl     the item that is actually storing the properties
        @param jobDataset   a dataset that provides the unique identity for
                            this job.  
        @param name     the value for the NAME property.
        @param inputs   a list of the output datasets (as Dataset list)
        @param outputs  a list of the output datasets (as Dataset list)
        @param triggerHandler  a TriggerHandler instance
        """
        BasicBlackboardItem.__init__(self, impl, name)
        if inputs:  self._setDatasets(impl, self.INPUT, inputs)
        if outputs:  self._setDatasets(impl, self.OUTPUT, outputs)
        if jobDataset:
            impl._setProperty(self.JOBIDENTITY, jobDataset.toPolicy())

        self.triggerHandler = triggerHandler

    def _setDatasets(self, impl, key, datasets):
        dsps = []
        if not isinstance(datasets, list):
            datasets = [datasets]
        for ds in datasets:
            dsps.append(ds.toPolicy())
        impl._setProperty(key, dsps)

    def getInputDatasets(self):
        return self._getDatasets(self.INPUT)

    def getOutputDatasets(self):
        return self._getDatasets(self.OUTPUT)

    def _getDatasets(self, key):
        dss = self.getProperty(key)
        out = []
        if dss:
            if not isinstance(dss, list):
                dss = [dss]
            for ds in dss:
                out.append(Dataset.fromPolicy(ds))
        return out

    def getJobIdentity(self):
        """
        return a Dataset instance representing the unique identity of the Job.
        """
        return Dataset.fromPolicy(self.getProperty(self.JOBIDENTITY))

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
    def setPipelineId(self, id):
        self._setProperty(self.PIPELINEID, _encodeId(id))

    def getPipelineId(self):
        return _decodeId(self.getProperty(self.PIPELINEID))

    def isSuccessful(self):
        return self.getProperty(self.SUCCESS)

    def markSuccessful(self, success=True):
        self._setProperty(self.SUCCESS, success)

    @staticmethod
    def createItem(jobDataset, name, inputs=None, outputs=[], 
                   triggerHandler=None, props=None):
        """
        create a BlackboardItem with the given properties
        @param jobDataset      a dataset that provides the unique identity for
                                 this job.  
        @param name            the item name
        @param inputs          a list of the output datasets (as Dataset list)
        @param outputs         a list of the output datasets (as Dataset list)
        @param triggerHandler  a TriggerHandler instance
        @param props           a dictionary of properties
        """
        impl = PolicyBlackboardItem()
        if props:
            for key in props.keys():
                impl._setProperty(key, props[key])
        out = JobItem(impl, jobDataset, name, inputs, outputs, triggerHandler)
        return out

class Props(object):
    """
    an enumeration of standard BlackboardItem property names
    """
    NAME     = BasicBlackboardItem.NAME
    SUCCESS  =     DataProductItem.SUCCESS
    DATASET  =     DataProductItem.DATASET
    INPUT    =             JobItem.INPUT
    OUTPUT   =             JobItem.OUTPUT


__all__ = "BlackboardItem DictBlackboardItem PolicyBlackboardItem ImplBlackboardItem BasicBlackboardItem DataProductItem PipelineItem JobItem Props".split()
