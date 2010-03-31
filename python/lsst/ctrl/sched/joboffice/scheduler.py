"""
scheduler classes
"""
from __future__ import with_statement

from lsst.ctrl.sched.blackboard import Blackboard, Props, JobItem, DataProductItem
from lsst.ctrl.sched.base import _AbstractBase
from triggers import Trigger
from triggerHandlers import FilesetTriggerHandler
from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.pex.logging import Log

import os, time

class Scheduler(_AbstractBase):
    """
    an abstract class that creates and schedules jobs that can be sent to
    pipelines.

    The scheduler is alerted to the availability of new datasets via its
    processDataset() function.  Its responsibility is two-fold:
      1.  check to see if the dataset is a trigger for any of the Jobs
            in the blackboards jobsPossible queue.
      2.  if necessary, create a new job to be added to the jobsPossible
            queue.
    After all available datasets have been processed, the makeJobsAvailable()
    can be called to schedule the jobs that are ready to go; this is done by
    moving the jobs from the jobsPossible queue for which
    all of its trigger datasets have been processed to the jobsAvailable
    queue.  
    """

    def __init__(self, blackboard, fromSubclass=False):
        """
        create the scheduler
        @param blackboard   the blackboard that this scheduler will update.
        """
        self._checkAbstract(fromSubclass, "Scheduler")
        self.bb = blackboard

    def processDataset(self, dataset, success):
        """
        note that the given trigger dataset is now available and update
        the jobs on the jobsPossible queue.  A trigger dataset is a dataset
        that must be made available before a job can start.  This function
        is thus responsible for recording this fact.  This function may also
        place new jobs on the jobsPossible list based on the availability of
        this dataset.
        @param dataset    the trigger dataset that is now available.
        @param success    True if the dataset was successfully created.
        """
        self._notImplemented("processDataset")

    def makeJobsAvailable(self):
        """
        schedule all jobs that are ready to go by moving them from the
        jobsPossible queue to the jobsAvailabe queue.  This should be done
        for jobs in the jobsPossible queue for which all their trigger
        datasets have been seen via processDataset().
        """
        self._notImplemented("makeJobsAvailable")

class DataTriggeredScheduler(Scheduler):
    """
    a Scheduler that uses a set of policy-described triggers to schedule its
    jobs.
    """

    def __init__(self, blackboard, policy, logger=None):
        """
        create the scheduler
        @param blackboard  the blackboard that this scheduler will update.
        @param policy      the "schedule" policy used to configure this
                              scheduler
        @param logger      a log to use for messages
        """
        Scheduler.__init__(self, blackboard, True)
        self.log = logger

        defpol = DefaultPolicyFile("ctrl_sched",
                                   "DataTriggeredScheduler_dict.paf",
                                   "policies")
        defpol = Policy.createPolicy(defpol, defpol.getRepositoryPath(), False)
        policy.mergeDefaults(defpol)

        self.triggers = []
        trigps = policy.getArray("trigger")
        for trigp in trigps:
            self.triggers.append(Trigger.fromPolicy(trigp))

        self.inputdata = []
        inpps = policy.getArray("jobDataset")
        for dsp in inpps:
            self.inputdata.append(Trigger.fromPolicy(dsp))

        self.nametmpl = None
        pol = policy.get("jobName")
        self.defaultName = pol.getString("default")
        if pol.exists("template"):
            self.nametmpl = pol.getString("template")
        self.nameNumber = pol.getInt("initCounter")

    def processDataset(self, dataset, success=True):
        """
        note that the given trigger dataset is now available and update
        the jobs on the jobsPossible queue.  A trigger dataset is a dataset
        that must be made available before a job can start.  This function
        is thus responsible for recording this fact.  This function may also
        place new jobs on the jobsPossible list based on the availability of
        this dataset.
        @param dataset    the trigger dataset that is now available.
        @param success    True if the dataset was successfully created.
        """

        # determine if this is a trigger dataset
        recognized = None
        for trigger in self.triggers:
            recognized = trigger.recognize(dataset)
            if recognized:
                break
        if not recognized:
            self._debug("Dataset not needed: %s", dataset)
            return False

        with self.bb.queues:
            product = DataProductItem.createItem(dataset, success)
            self.bb.queues.dataAvailable.append(product)

        with self.bb.queues:

            # determine if this job is needed by any jobs in the
            # jobPossible queue
            needed = False
            for i in xrange(self.bb.queues.jobsPossible.length()):
                job = self.bb.queues.jobsPossible.get(i)

                if job.setNeededDataset(recognized):
                    needed = True
                    self._debug("Dataset %s needed for Job %s", (dataset, job))

            # if not needed by any current possible jobs, create
            # a new job and put it on the jobsPossible queue
            if not needed:
                name = self.createName(recognized)
                inputs = []
                for filt in self.inputdata:
                    inputs.extend(filt.listDatasets(recognized))

                trighdlr = \
                      FilesetTriggerHandler(trigger.listDatasets(recognized))
                
                job = JobItem.createItem(name, inputs, trighdlr)
                job.setNeededDataset(recognized)
                self.bb.queues.jobsPossible.append(job)

    def createName(self, dataset):
        """
        create a job name based on a trigger dataset
        """
        out = None
        if self.nametmpl:
            ids = dict(dataset.ids)
            ids["type"] = dataset.type
            try:
                out = self.nametmpl % ids
            except KeyError:
                self._debug("Trouble creating name via %s % %s",
                            (self.nametmpl, ids))
                
        if not out:
            out = "%s-%s" % (self.defaultName, self.nameNumber)
            self.nameNumber += 1
        return out

    def _debug(self, msg, args=None):
        if self.log:
            if args:  msg = msg % args
            self.log.log(Log.DEBUG, msg)

    def makeJobsAvailable(self):
        """
        schedule all jobs that are ready to go by moving them from the
        jobsPossible queue to the jobsAvailabe queue.  This should be done
        for jobs in the jobsPossible queue for which all their trigger
        datasets have been seen via processDataset().
        """
        with self.bb:
            jobs = []
            for i in xrange(self.bb.queues.jobsPossible.length()):
                job = self.bb.queues.jobsPossible.get(i)
                if job.isReady():
                    jobs.append(job)

            for job in jobs:
                self.bb.makeJobAvailable(job)



            
