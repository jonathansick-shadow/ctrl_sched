"""
classes for evaluating triggers
"""
from __future__ import with_statement

from lsst.pex.policy import Policy
from lsst.pex.logging import Log
from id import IDFilter
from lsst.ctrl.sched.blackboard.base import _AbstractBase

import os

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
        if datasetType is not None and not isinstance(datasetType, list):
            datasetType = [datasetType]
        self.dataTypes = datasetType

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

