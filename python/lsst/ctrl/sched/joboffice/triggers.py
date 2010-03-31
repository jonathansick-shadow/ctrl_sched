"""
classes for evaluating triggers
"""
from __future__ import with_statement

from lsst.pex.policy import Policy
from lsst.pex.logging import Log
from id import IDFilter
from lsst.ctrl.sched.dataset import Dataset
from lsst.ctrl.sched.base import _AbstractBase

import os, copy

class Trigger(_AbstractBase):
    """
    an abstract class representing a dataset trigger.  When passed a 
    Dataset instance via the recognize function, it will check it against
    a set of criteria.  If the criteria are met, the dataset is recognized
    and the trigger is considered "pulled"
    """

    def __init__(self, fromSubclass=False):
        """
        instantiate this base class.  
        """
        self._checkAbstract(fromSubclass, "Trigger")
        self.isstatic = False

    def hasPredictableDatasetList(self):
        """
        return True if the list of datasets returned by listDatasets() 
        should be considered the complete list of datasets that can trigger
        this filter.  If False, calling listDatasets() may raise an 
        exception if a set cannot be generated. 
        """
        return self.isstatic

    def listDatasets(self, template=None):
        """
        return a list of all the datasets that will be returned by recognize()
        @param template    a Dataset instance representing a template for 
                              identifiers and types not constrained by this 
                              trigger.  If the given Dataset is not recognized,
                              an empty set is returned.
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
    def fromPolicy(policy):
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
            raise RuntimeError("programmer error class name lookup not implemented")

        return cls.fromPolicy(policy)
        
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

    def listDatasets(self, template=None):
        """
        return a list of all the datasets that will be returned by recognize().
        This implementation returns a set of datasets made up of all 
        combinations of the dataset types and allowed identifiers (for those
        identifiers that have a closed set of values).  
        @param template    a Dataset instance representing a template for 
                              identifiers and types not constrained by this 
                              trigger.  If the given Dataset is not recognized,
                              an empty set is returned.
        """
        if template:
            # the template is used to set values of identifiers not of 
            # interest to this filter and identifiers that can't be 
            # reduced to a closed set.  
            # if not self.recognize(template):
            #     return []
            types = [ template.type ]
        else:
            if not self.dataTypes:
                raise RuntimeError("can't close set without template dataset")
            types = list(self.dataTypes)
            template = Dataset(types[0])

        # get a list of allowed id values
        idvals = {}
        for id in self.idfilts.keys():
            idvals[id] = []
            for filt in self.idfilts[id]:
                if filt.hasStaticValueSet():
                    idvals[id].extend(filt.allowedValues())
                elif not template.ids.has_key(id):
                    # template datsets unable to close the set
                    raise RuntimeError("can't close identifier set for " + id)
            if len(idvals[id]) == 0:
                del idvals[id]

        # these are the idnames, then, we are varying; and the number of 
        # values for each.  The total number of datasets returned will then
        # be len(types) * PI(valcnt.values())
        idnames = idvals.keys()
        valcnt = {}
        for id in idnames:
            valcnt[id] = len(idvals[id])

        out = []
        for type in types:
            # initialize our multidimensional iterator
            iter = valcnt.fromkeys(idnames, 0)

            # quit adding when the last axis of the iterator meets its limit
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

        return out
    

    @staticmethod
    def fromPolicy(policy):
        """
        @param policy   a trigger Policy instance describing the 
                            simple trigger
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

        return SimpleTrigger(dataTypes, ids=idfilts)

Trigger.classLookup["Simple"] = SimpleTrigger
Trigger.classLookup["SimpleTrigger"] = SimpleTrigger

