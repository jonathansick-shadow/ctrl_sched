"""
classes for tracking a set of triggers for a Job
"""
from __future__ import with_statement

from lsst.pex.policy import Policy
from lsst.pex.logging import Log
from lsst.ctrl.sched.dataset import Dataset
from lsst.ctrl.sched.base import _AbstractBase

import os

class TriggerHandler(_AbstractBase):
    """
    an abstract class for managing a set of triggers datasets for a job.
    If a dataset has been recognized by as a trigger dataset, it can be
    added to this handler.  When the handler has received sufficient trigger
    datasets, its isReady() function will return True.
    """

    def __init__(self, fromSubclass=False):
        """
        instantiate this base class.  
        """
        self._checkAbstract(fromSubclass, "TriggerHandler")

    def addDataset(self, dataset):
        """
        note that the given dataset is a trigger for this job.
        """
        self._notImplemented("addDataset")

    def isReady(self):
        """
        return True if all expected trigger datasets have been added so
        that the job can be scheduled.

        This implementation always returns False.
        """
        return False


class FilesetTriggerHandler(TriggerHandler):
    """
    a TriggerHandler which is initialized with a set of static set datasets.
    When all these datasets have been added, the job is ready.
    """

    def __init__(self, dslist=None):
        """
        initialize the handler
        @param dslist    the list of Datasets to listen for
        """
        if dslist is None:
            dslist = []
        if not isinstance(dslist, list):
            dslist = [dslist]
        self.dids = set(map(lambda d: d.toString(False), dslist))

    def getNeededDatasetCount(self):
        return len(self.dids)

    def isNeededDataset(self, dataset):
        """
        return true if the given dataset is needed for this job
        """
        return dataset.toString(False) in self.dids

    def addDataset(self, dataset):
        """
        note that the given dataset is a trigger for this job.
        @return bool   True if the dataset was needed but not added until now
        """
        id = dataset.toString(False)
        if id in self.dids:
            self.dids.remove(id)
            return True
        return False

    def isReady(self):
        """
        return True if all expected trigger datasets have been added so
        that the job can be scheduled.
        """
        return len(self.dids) == 0

