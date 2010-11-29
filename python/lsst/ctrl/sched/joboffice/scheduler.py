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
scheduler classes
"""
from __future__ import with_statement

from lsst.ctrl.sched import Dataset, utils
from lsst.ctrl.sched.blackboard import Blackboard, Props, JobItem, DataProductItem
from lsst.ctrl.sched.base import _AbstractBase
from triggers import Trigger
from triggerHandlers import FilesetTriggerHandler
from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.pex.logging import Log

import os, time

# NOTE:  the two schedule implementations do not use the Trigger API in
# a consistent way; the Trigger API needs to be reworked.

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

    # for loading implementations specified in policy
    classLookup = {}

    def __init__(self, blackboard, logger=None, fromSubclass=False):
        """
        create the scheduler
        @param blackboard   the blackboard that this scheduler will update.
        @param logger       a log to use for messages
        """
        self._checkAbstract(fromSubclass, "Scheduler")
        self.bb = blackboard
        self.log = logger

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

    def _debug(self, msg, args=None):
        self._tell(Log.DEBUG, msg, args)

    def _warn(self, msg, args=None):
        self._tell(Log.WARN, msg, args)

    def _tell(self, lev, msg, args=None):
        if self.log:
            if args:  msg = msg % args
            self.log.log(lev, msg)

    def makeJobsAvailable(self):
        """
        schedule all jobs that are ready to go by moving them from the
        jobsPossible queue to the jobsAvailabe queue.  This should be done
        for jobs in the jobsPossible queue for which all their trigger
        datasets have been seen via processDataset().
        """
        with self.bb:
            ready = []
            for i in xrange(self.bb.queues.jobsPossible.length()):
                job = self.bb.queues.jobsPossible.get(i)
                if job.isReady():
                    ready.append(job)

            for job in ready:
                self.bb.makeJobAvailable(job)

    @staticmethod
    def fromPolicy(blackboard, policy, logger=None):
        """
        a factory method for creating a Scheduler instance based on a
        schedule policy
        @param blackboard  the blackboard that this scheduler will update.
        @param policy      the "schedule" policy used to configure this
                              scheduler
        @param logger      a log to use for messages
        """
        clsname = "DataTriggered"
        if policy.exists("className"):
            clsname = policy.getString("className")

        cls = None
        if Scheduler.classLookup.has_key(clsname):
            cls = Scheduler.classLookup[clsname]
        else:
            cls = utils.importClass(clsname)
            if not issubclass(cls, Scheduler):
               raise TypeError("Policy schedule.className is not a Scheduler: "
                               + clsname)

        return cls(blackboard, policy, logger)

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
        Scheduler.__init__(self, blackboard, logger, True)

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
        inpps = policy.getArray("job.input")
        for dsp in inpps:
            self.inputdata.append(Trigger.fromPolicy(dsp, True))

        self.outputdata = []
        outpps = policy.getArray("job.output")
        for dsp in outpps:
            self.outputdata.append(Trigger.fromPolicy(dsp, True))

        self.jobIdConf = None
        if policy.exists("job.identity"):
            self.jobIdConf = policy.getPolicy("job.identity")

        self.nametmpl = None
        pol = policy.get("job.name")
        self.defaultName = pol.getString("default")
        if pol.exists("template"):
            self.nametmpl = pol.getString("template")
        self.nameNumber = pol.getInt("initCounter")

    def processDataset(self, dataset, success=None):
        """
        note that the given trigger dataset is now available and update
        the jobs on the jobsPossible queue.  A trigger dataset is a dataset
        that must be made available before a job can start.  This function
        is thus responsible for recording this fact.  This function may also
        place new jobs on the jobsPossible list based on the availability of
        this dataset.
        @param dataset    the trigger dataset that is now available.
        @param success    True if the dataset was successfully created.  If
                             None (default), the dataset valid flag will be
                             the indicator of success.
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

        if success is None:
            success = dataset.valid

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
                inputs = []
                for filt in self.inputdata:
                    inputs.extend(filt.listDatasets(recognized))
                outputs = []
                for filt in self.outputdata:
                    outputs.extend(filt.listDatasets(recognized))

                trighdlr = \
                      FilesetTriggerHandler(trigger.listDatasets(recognized))

                jobds = self._determineJobIdentity(outputs, inputs)
                name = self.createName(jobds)
                
                job = JobItem.createItem(jobds, name, inputs,outputs, trighdlr)
                job.setNeededDataset(recognized)
                self.bb.queues.jobsPossible.append(job)

    def _determineJobIdentity(self, outputs, inputs=None):
        # return an identifier for the job implied by the outputs and inputs.
        # this identifier is returned in the form of a Dataset type (even
        # though, semantically, it represents a job.
        
        if inputs is None:  inputs = []
        
        if self.jobIdConf:
            # determine our template dataset for our identity
            template = None
            if self.jobIdConf.exists("templateType"):
                # find first dataset (in output, then input) matching
                # this dataset type.
                type = self.jobIdConf.getString("templateType")
                selecttype = lambda d: d.type == type
                template = filter(selecttype, outputs)
                if len(template) == 0: template = filter(selecttype, inputs)
                if len(template) > 0: template = template[0]
            if not template:
                # default to the first output (then input) dataset
                template = len(outputs) > 0 and outputs[0] or inputs[0]

            out = Dataset(template.type)
            if self.jobIdConf.exists("type"):
                out.type = self.jobIdConf.getString("type")
            if self.jobIdConf.exists("id"):
                out.ids = {}
                for id in self.jobIdConf.getStringArray("id"):
                    out.ids[id] = template.ids[id]

            # the identity dataset is complete
            return out

        elif len(outputs) > 0:
            return outputs[0]
        elif len(inputs) > 0:
            return inputs[0]
        else:
            return Dataset("unknown")

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


class ButlerTriggeredScheduler(Scheduler):
    """
    A scheduler that uses a data butler registry to create jobs from i
    incoming prerequisite datasets.
    """

    def __init__(self, blackboard, policy, logger=None):
        """
        create the scheduler
        @param blackboard  the blackboard that this scheduler will update.
        @param policy      the "schedule" policy used to configure this
                              scheduler
        @param logger      a log to use for messages
        """
        Scheduler.__init__(self, blackboard, logger, True)

        defpol = DefaultPolicyFile("ctrl_sched",
                                   "ButlerTriggeredScheduler_dict.paf",
                                   "policies")
        defpol = Policy.createPolicy(defpol, defpol.getRepositoryPath(), False)
        policy.mergeDefaults(defpol)

        self.triggers = []
        trigps = policy.getArray("trigger")
        for trigp in trigps:
            self.triggers.append(Trigger.fromPolicy(trigp))

        self.inputdata = []
        inpps = policy.getArray("job.input")
        for dsp in inpps:
            self.inputdata.append(Trigger.fromPolicy(dsp, True))

        self.outputdata = []
        outpps = policy.getArray("job.output")
        for dsp in outpps:
            self.outputdata.append(Trigger.fromPolicy(dsp, True))

        self.jobIdConf = None
        if policy.exists("job.identity"):
            self.jobIdConf = policy.getPolicy("job.identity")

        self.nametmpl = None
        pol = policy.get("job.name")
        self.defaultName = pol.getString("default")
        if pol.exists("template"):
            self.nametmpl = pol.getString("template")
        self.nameNumber = pol.getInt("initCounter")

    def processDataset(self, dataset, success=None):
        """
        note that the given trigger dataset is now available and update
        the jobs on the jobsPossible queue.  A trigger dataset is a dataset
        that must be made available before a job can start.  This function
        is thus responsible for recording this fact.  This function may also
        place new jobs on the jobsPossible list based on the availability of
        this dataset.
        @param dataset    the trigger dataset that is now available.
        @param success    True if the dataset was successfully created.  If
                             None (default), the dataset valid flag will be
                             the indicator of success.
        """

        # determine what jobs this dataset is a prerequisite for
        jobs = []
        for trigger in self.triggers:
            # Note these triggers are actually listing possible job IDs
            jobs.extend(trigger.listDatasets(dataset))
        if not jobs:
            self._debug("Dataset not needed: %s", dataset)
            return False

        if success is None:
            success = dataset.valid

        with self.bb.queues:
            product = DataProductItem.createItem(dataset, success)
            self.bb.queues.dataAvailable.append(product)

        with self.bb.queues:

            # iterate over jobs and determine if it has been referenced
            # before
            for jobid in jobs:
                found = False
                for i in xrange(self.bb.queues.jobsPossible.length()):
                    candidate = self.bb.queues.jobsPossible.get(i)
        
                    if jobid == candidate.getJobIdentity():
                        found = True
                        if not candidate.setNeededDataset(dataset):
                            self._warn("Dataset %s not needed for matched "+
                                       "Job %s", (dataset, jobid))
                        else:
                            self._debug("Dataset %s needed for Job %s",
                                        (dataset, jobid))
                        
                if not found:
                    # nominate this job by adding it to jobsPossible
                    inputs = []
                    prereqs = []
                    for filt in self.inputdata:
                        dss = filt.listDatasets(jobid)
                        inputs.extend(dss)
                        if hasattr(filt, "prereq") and filt.prereq:
                            prereqs.extend(dss)
                    trighdlr = FilesetTriggerHandler(prereqs)
                    
                    outputs = []
                    for filt in self.outputdata:
                        outputs.extend(filt.listDatasets(jobid))

                    name = self.createName(jobid)
                    candidate = JobItem.createItem(jobid, name, inputs,outputs,
                                                   trighdlr)
                    candidate.setNeededDataset(dataset)
                    self.bb.queues.jobsPossible.append(candidate)
                    
    def createName(self, jobid):
        """
        create a job name based on a job id
        """
        out = None
        if self.nametmpl:
            ids = dict(jobid.ids)
            ids["type"] = jobid.type
            try:
                out = self.nametmpl % ids
            except KeyError:
                self._debug("Trouble creating name via %s % %s",
                            (self.nametmpl, ids))
                
        if not out:
            out = "%s-%s" % (self.defaultName, self.nameNumber)
            self.nameNumber += 1
        return out

Scheduler.classLookup["ButlerTriggered"] = ButlerTriggeredScheduler
Scheduler.classLookup["ButlerTriggeredScheduler"] = ButlerTriggeredScheduler
