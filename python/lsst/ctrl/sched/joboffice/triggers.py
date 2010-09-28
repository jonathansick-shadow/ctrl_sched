"""
classes for evaluating triggers
"""
from __future__ import with_statement

from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.daf.persistence import butlerFactory, Mapper
from lsst.pex.logging import Log
from id import IDFilter
from lsst.ctrl.sched import Dataset, utils
from lsst.ctrl.sched.base import _AbstractBase

import os, copy

class Trigger(_AbstractBase):
    """
    an abstract class representing a set of datasets that will be used to
    create a processing job.  This class is used to describe two kinds of
    datasets (which kind depends on the isTrigger options passed to the
    constructor).  The class can describe "trigger datasets"--datasets
    that are required to exist in order for the job to commence:  subsequently,
    when a Dataset instance is passed via the recognize function, it will
    check it against a set of criteria.  If the criteria are met, the dataset
    is recognized, the trigger is considered "pulled", and this one
    processing prerequisite is satisfied.  The class can also be used to
    represent input or output datasets for the Job triggered by a recognized
    dataset.  In this case, listDatasets is important for generating the
    full set implied by a particular triggering dataset.
    """

    def __init__(self, isTrigger=True, fromSubclass=False):
        """
        instantiate this base class.  
        """
        self._checkAbstract(fromSubclass, "Trigger")
        self.isstatic = False
        self.isTrigger = isTrigger

    def hasPredictableDatasetList(self):
        """
        return True if the list of datasets returned by listDatasets() 
        should be considered the complete list of datasets that can trigger
        this filter.  If False, calling listDatasets() may raise an 
        exception if a set cannot be generated. 
        """
        return self.isstatic

    def listDatasets(self, template):
        """
        return a list of applicable datasets corresponding to the IDs
        associated with a template dataset.  (See class documentation
        for caveats in this function's behavior depending on how the
        instance was constructed.
        @param template    a Dataset instance that would trigger a processing
                              job.  The identifiers associated with the
                              template define a job (and constrain its specific
                              inputs and outputs)
        """
        self._notImplemented("listDatasets")
    

    def recognize(self, dataset):
        """
        return a list of datasets that are expected to be available when 
        the input dataset is recognized or None if the dataset is not 
        recognized.

        This default implementation always returns None

        @param dataset    a Dataset instance to test
        """
        return None

    classLookup = { }

    @staticmethod
    def fromPolicy(policy, isIOdata=False):
        """
        a factory method for creating a Trigger instance based on a 
        trigger policy.
        @param policy   the policy that describes the trigger
        """
        clsname = "SimpleTrigger"
        if policy.exists("className"):
            clsname = policy.getString("className")

        cls = None
        if Trigger.classLookup.has_key(clsname):
            cls = Trigger.classLookup[clsname]

        else:
            # lookup a fully qualified class
            cls = utils.importClass(clsname)
            if not issubclass(cls, Trigger):
                raise TypeError("Policy className is not a Trigger type: " +
                                clsname)

        return cls.fromPolicy(policy, isIOdata)
        
class SimpleTrigger(Trigger):
    """
    a Trigger implementation
    """

    def __init__(self, datasetType=None, ids=None, **kw):
        """
        @param datasetType   the type of dataset to look for.  This can 
                               either be a single type name or list of 
                               names
        @param ids           a dictionary mapping identifier names to 
                               IDFilter instances
        @param *             additional named parameters are taken as 
                               identifiers to be set with the given values
        """
        Trigger.__init__(self, fromSubclass=True)
        if datasetType is not None and not isinstance(datasetType, list):
            datasetType = [datasetType]
        self.dataTypes = datasetType
        if self.dataTypes is not None:
            self.isstatic = True

        self.idfilts = None
        if kw:
            if ids is None:
                ids = {}
            for key in kw.keys():
                ids[key] = kw[key]

        if ids: 
            self.idfilts = {}
            for id in ids.keys():
                self.idfilts[id] = ids[id]
                if isinstance(self.idfilts[id], list):
                    self.idfilts[id] = list( self.idfilts[id] )
                else:
                    self.idfilts[id] = [ self.idfilts[id] ]
                if self.isstatic and \
                   len(filter(lambda i: not i.hasStaticValueSet(), 
                              self.idfilts[id])) > 0:
                    self.isstatic = False

        
    def recognize(self, dataset):
        """
        return a list of datasets that are expected to be available when 
        the input dataset is recognized or None if the dataset is not 
        recognized.
        @param dataset    a Dataset instance
        """
        if self.dataTypes is not None and \
           dataset.type not in self.dataTypes:
            return None
    
        # attempt to recognize the ids
        if self.idfilts is not None:
            if dataset.ids is None:
                return None
            
            # iterate through the identifier filters, passing through
            # the appropriate identifiers from the dataset
            for idname in self.idfilts.keys():
                if not dataset.ids.has_key(idname):
                    return None
                
                # we're looking for this one; the identifier is 
                # recognized if any of filters return True
                recognized = False
                for filt in self.idfilts[idname]:
                    if filt.recognize(dataset.ids[idname]) is not None:
                        recognized = True
                        break
                if not recognized:
                    return None

        # all tests pass; return this dataset
        return dataset

    def listDatasets(self, template):
        """
        return a list of applicable datasets corresponding to the IDs
        associated with a template dataset.  
        @param template    a Dataset instance that would trigger a processing
                              job.  The identifiers associated with the
                              template defin a job (and constrain its specific
                              inputs and outputs)
        """

        # the main difference between SimpleTrigger that describes a trigger
        # dataset and one that describes input/output data is that the latter
        # description is complete in terms of defining all of the identifiers
        # that select the data.  For trigger datasets, only the minimal
        # identifiers important for the trigger are included in the description.
        # Thus, in the latter case, the template provides the full set of
        # identifiers that are relevent to the job.  Another diference is that
        # when we are describing input/output data, the dataset type of that
        # data won't necessarily match that of trigger dataset.

        # if not self.recognize(template):
        #     return []

        if not self.isTrigger:
            # we are listing input/output data; restrict the ids to those
            # defined in this class instance's data
            template = copy.deepcopy(template)
            if template.ids is None: template.ids = {}
            for id in template.ids.keys():
                if not id in self.idfilts.keys():
                    del template.ids[id]

            # this class defines what types are included in the list
            types = self.dataTypes
        else:
            # we are listing only the trigger data sets defined by this trigger.
            # The template controls what datatypes get into the output list.
            types = [ template.type ]

        # get a list of allowed id values
        idvals = {}
        if self.idfilts is not None:
           for id in self.idfilts.keys():
             idvals[id] = []
             for filt in self.idfilts[id]:
                if filt.hasStaticValueSet():
                    # filter provides closed set of allowed values
                    idvals[id].extend(filt.allowedValues())
                elif template.ids.has_key(id):
                    # take the value from the template the only allowed value
                    idvals[id].append(template.ids[id])
                else:
                    # template datsets unable to close the set
                    raise RuntimeError("can't close identifier set for " + id)
             if len(idvals[id]) == 0:
                del idvals[id]

        # these are the ids that we need to loop over; for the others, we
        # just take the value from the template
        idnames = idvals.keys()
        out = []
        if len(idnames) > 0:

            # iterate through all combinations of allowed id values.
            # The total number of datasets returned will then
            # be len(types) * PI(valcnt.values())
            valcnt = {}
            for id in idnames:
                valcnt[id] = len(idvals[id])

            for type in types:
                # initialize our multidimensional iterator
                iter = valcnt.fromkeys(idnames, 0)

                # quit adding when the last axis of the iterator meets its
                # limit
                while iter[idnames[-1]] < valcnt[idnames[-1]]:

                    # clone the template
                    ds = copy.deepcopy(template)
                    ds.type = type
                    if ds.ids is None:
                        ds.ids = {}

                    # set the values of the identifiers in the dataset
                    for id in iter.keys():
                        ds.ids[id] = idvals[id][iter[id]]
                    out.append(ds)

                    # increment the iterator
                    for i in xrange(len(idnames)):
                        iter[idnames[i]] += 1
                        if iter[idnames[i]] < valcnt[idnames[i]]:
                            break
                        if i == len(idnames) - 1:
                            break
                        iter[idnames[i]] = 0

        else:
            # this trigger places no constaints on the files; return a
            # single dataset list based entirely on template
            for type in types:
                ds = copy.deepcopy(template)
                ds.type = type
                out.append(ds)

        return out
    

    @staticmethod
    def fromPolicy(policy, isIOdata=False):
        """
        @param policy     a trigger Policy instance describing the 
                            simple trigger
        @param isIOdata   True if the policy describes either input or
                            output data; False if it describes the
                            trigger datasets.
        """
        dataTypes = None
        if policy.exists("datasetType"):
            dataTypes = policy.getArray("datasetType")

        idfilts = None
        if policy.exists("id"):
            idfilts = {} 
            idps = policy.getArray("id")
            for idp in idps:
                idfilt = IDFilter.fromPolicy(idp)
                if not idfilts.has_key(idfilt.name):
                    idfilts[idfilt.name] = []
                idfilts[idfilt.name].append(idfilt)

        out = SimpleTrigger(dataTypes, ids=idfilts)
        out.isTrigger = not isIOdata
        return out

Trigger.classLookup["Simple"] = SimpleTrigger
Trigger.classLookup["SimpleTrigger"] = SimpleTrigger

class MapperTrigger(Trigger):
    """
    a Trigger implementation in which datasets are described in terms of a
    dataset type and a mapping function that maps a set of IDs into another
    set.
    """

    def __init__(self, matchTypes, mapper, lookupType, granularity,
                 triggerIds=None, targetIds=None, targetType=None):
        """
        create the trigger.
        @param matchTypes    the list of types of datasets to look for.  If
                               empty, any type will be matched.
        @param mapper        an instance of lsst.daf.persistence.mapper.Mapper
                               the provides the required ID mapping
        @param granularity   the ID key provided to the mapper that sets
                               the needed ID granularity in the output mapping.
        @param lookupType    the dataset type ot provide the mapper to
                               provide the needed mapping
        @param granularity   the ID key to use to set the mapper's ID
                               granularity.
        @param triggerIds    the set of ID names that will be the input into
                               the mapping.  If None, an empty list is assumed.
        @param targetIds     the set of names for the IDS that are needed out 
                               of the mapping to create a list of relevent
                               datasets.  If None, an empty list is assumed.
        @param targetType    the dataset to map to.  If None, the input type
                               will be assumed.
        @param prereq        if True, consider the output datasets implied by
                               the mapping a job prerequisite
        """
        Trigger.__init__(self, fromSubclass=True)

        if matchTypes is None:
            matchTypes = []
        elif not isinstance(matchTypes, list):
            matchTypes = [ matchTypes ]
        self.matchTypes = matchTypes
        if not isinstance(mapper, Mapper):
            raise ValueError("mapper parameter value not a Mapper:" +
                             type(mapper))
        self.mapper = mapper
        self.lookupType = lookupType
        self.gran = granularity
        if triggerIds is None:
            triggerIds = []
        self.triggerIds = triggerIds
        if targetIds is None:
            targetIds = []
        self.targetIds = targetIds
        self.targetType = targetType

        self.prereq = False

    def listDatasets(self, template):
        """
        return a list of applicable datasets corresponding to the IDs
        associated with a template dataset.  (See class documentation
        for caveats in this function's behavior depending on how the
        instance was constructed.
        @param template    a Dataset instance that would trigger a processing
                              job.  The identifiers associated with the
                              template define a job (and constrain its specific
                              inputs and outputs)
        """
        if self.matchTypes and template.type not in self.matchTypes:
            return []
        
        trigVals = {}
        for id in self.triggerIds:
            trigVals[id] = None
            if id in template.ids:
                trigVals[id] = template.ids[id]

        out = []
        outType = self.targetType or template.type
        for targetVals in self._mapIds(trigVals):
            ids = {}
            if len(targetVals) != len(self.targetIds):
                raise ValueError("Wrong number of values; " +
                                 "_mapIds programming error?")
            for i in xrange(len(self.targetIds)):
                ids[self.targetIds[i]] = targetVals[i]
            out.append(Dataset(outType, ids=ids))

        return out


    def _mapIds(self, triggerValues):
        return self.mapper.queryMetadata(self.lookupType, self.gran,
                                         self.targetIds, triggerValues)

    def recognize(self, dataset):
        """
        return a list of job identifiers that this dataset is recognized to
        be a prerequesite for .  

        @param dataset    a Dataset instance to test
        """
        jobs = self.listDatasets(dataset)
        return (jobs and dataset) or None

    @staticmethod
    def fromPolicy(policy, isIOdata=False):
        """
        @param policy     a trigger Policy instance describing the 
                            simple trigger
        @param isIOdata   True if the policy describes either input or
                            output data; False if it describes the
                            trigger datasets.
        """
        defPolFile = DefaultPolicyFile("ctrl_sched", "MapperTrigger_dict.paf",
                                       "policies")
        defPolicy = Policy.createPolicy(defPolFile,
                                        defPolFile.getRepositoryPath())
        policy.mergeDefaults(defPolicy.getDictionary())
        
        # create the mapper
        mapperPol = policy.get("mapper")
        clsname = mapperPol.get("className")
        mapperConfig = mapperPol.get("configuration")
        mapper = butlerFactory._importMapper(clsname, mapperConfig)
        
        lookupType = mapperPol.get("lookupType")
        granKey = mapperPol.get("idGranularity")

        matchTypes = None
        if policy.exists("datasetType"):
            matchTypes = policy.getArray("datasetType")
        triggerIds = None
        if policy.exists("triggerId"):
            triggerIds = policy.getArray("triggerId")
        targetType = None
        if policy.exists("targetType"):
            targetType = policy.get("targetType")
        targetIds = None
        if policy.exists("targetId"):
            targetIds = policy.getArray("targetId")

        out = MapperTrigger(matchTypes, mapper, lookupType, granKey,
                            triggerIds, targetIds, targetType)
        out.isTrigger = not isIOdata
        out.prereq = policy.get("prerequisites")
        return out

Trigger.classLookup["Mapper"] = MapperTrigger
Trigger.classLookup["MapperTrigger"] = MapperTrigger
        
        
