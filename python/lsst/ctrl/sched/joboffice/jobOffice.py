"""
jobOffice implementations
"""
from __future__ import with_statement

from lsst.ctrl.sched.blackboard import Blackboard, Props
from lsst.ctrl.sched.base import _AbstractBase
from lsst.ctrl.events import EventSystem
from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.pex.logging import Log

import os, time

class JobOffice(_AbstractBase):
    """
    an abstract class that is responsible for using a blackboard to track
    the progress of running pipelines and sending them jobs as needed.
    """

    def __init__(self, persistDir, fromSubclass=False):
        """
        create the JobOffice
        """
        self._checkAbstract(fromSubclass, "JobOffice")
        
        self.bb = Blackboard(persistDir)
        self.esys = EventSystem.getDefaultEventSystem()
    
    def run(self):
        """
        continuously listen for events from pipelines and schedule jobs
        accordingly.  This will only exit when this job office has received
        stop signal or the JobOffice determines that work has been completed.
        """
        self._notImplemented("run")

class _BaseJobOffice(JobOffice):
    """
    an abstract JobOffice that provides much of the implementation for
    policy-configured behavior.  
    """

    def __init__(self, rootdir, log=None, policy=None, defPolicyFile=None,
                 fromSubclass=False):
        """
        create the JobOffice
        @param rootdir        the root directory where job offices may set up
                                 its blackboard data.  This JobOffice will
                                 create a subdirectory here for its data with
                                 a name set by the "name" policy paramter.
        @param log            a logger to use; if None, the default logger will
                                 will be used.  A child log will be created.
        @param policy         the policy to use to configure this JobOffice
        @param defPolicyFile  the DefaultPolicyFile to use for defaults.  If
                                 this points to a dictionary, a policy
                                 validation will be done.  If None, an
                                 internally identified default policy file
                                 will be used.
        @param fromSubclass   the flag indicating that this constructor is
                                 being properly called.  Calls to this
                                 constructor from a subclass constructor should
                                 set this to True.
        """
        self._checkAbstract(fromSubclass, "_BasicJobOffice")

        # start by establishing policy data
        if not defPolicyFile:
            defPolicyFile = DefaultPolicyFile("ctrl_sched",
                                              "baseJobOffice_dict.paf",
                                              "policies")
        defaults = Policy.createPolicy(defPolicyFile,
                                       policyFile.getRepositoryPath(),
                                       True)
        if not policy:
            policy = Policy()
        self.policy = policy
        if defaults.canValidate():
            self.policy.mergeDefaults(defaults.getDictionary())
        else:
            self.policy.mergeDefaults(defaults)
            
        # instantiate parent class
        self.name = self.policy.get("name")
        persistDir = self.policy.get("persist.dir") % {"schedroot": rootdir, 
                                                       "name": self.name    }
        JobOffice.__init__(self, persistDir)

        # logger
        if not log:
            log = Log(Log.getDefaultLog(), self.name)
        self.log = log

        # initialize some data from policy
        self.initialWait = self.policy.get("listen.initialWait")
        self.emptyWait = self.policy.get("listen.emptyWait")
        self.dataTopics = self.policy.getArray("listen.dataReadyEvents")
        self.jobTopic = self.policy.get("listen.pipelineEvent")
    
    def run(self):
        """
        continuously listen for events from pipelines and schedule jobs
        accordingly.  This will only exit when this job office has received
        stop signal or the JobOffice determines that work has been completed.
        """
        while True:
            # listen for completed Jobs
            self.processDoneJobs()

            # look for available data to add to dataAvailable queue
            self.processDataEvents()

            # look for possibleJobs that are ready to go
            self.findAvailableJobs()

            # listen for pipelines ready to run and give them jobs
            self.allocateJobs()

    def processDoneJobs(self):
        """
        listen for done events from pipelines in progress and update
        their state in the blackboard.
        @return int   the number of jobs found to be done
        """
        out = 0
        constraint = "status='job:done'",
        jevent = self.esys.receiveEventWhere(self.jobTopic, constraint,
                                             self.initialWait)
        if not jevent:
            return 0

        while jevent:
            if self.processJobDoneEvent(jevent):
                out += 1
            jevent = self.esys.receiveEventsWhere(self.jobTopic, constraint,
                                                  self.emptyWait)

        return out

    def processJobDoneEvent(self, jevent):
        """
        process a job-done event.  If the event matches a job in the 
        jobsInProgress queue, the job will be moved to the jobsDone queue.
        @param jevent   the job event to process
        """
        if not isinstance(jevent, StatusEvent):
            # unexpected type; log a message?
            return False
        
        with self.bb:
            job = self.findOriginator(jevent.getOriginator())
            if not job:
                return False
            self.bb.markJobDone(job, jevent.getProperties().getString("success"))
        return True

    def processDataEvents(self):
        """
        receive and process all data events currently available.
        @return int   the number of events processed
        """
        out = 0
        devent = self.esys.receiveEvents(self.dataTopics, self.initialWait)
        if not devent:
            return 0

        while devent:
            if self.processDataEvent(devent):
                out += 1
            devent = self.esys.receiveEvents(self.dataTopics, self.emptyWait)

        return out

    def processDataEvent(self, event):
        """
        process an event indicating that one or more datasets are available.
        @param event    the data event.  
        @return bool    true if the event was processed.
        """
        self._notImplemented("processDataEvent")

    def findAvailableJobs(self):
        """
        move all jobs in the jobsPossible that are ready to go to the
        jobsAvailable queue.
        """
        self._notImplemented("findAvailableJobs")

    def allocateJobs(self):
        """
        listen for pipelines ready to run and give them jobs to do
        """
        self.receiveReadyPipelines()

        out = 0
        with self.bb:
            while not self.bb.queus.pipelinesReady.isEmpty() and \
                  not self.bb.queus.jobsAvailable.isEmpty():

                with self.bb.pipelinesReady:
                    pipe = self.bb.pipelinesReady.pop()
                    job = self.bb.queus.jobsAvailable.get(0)

                    # send a command to that pipeline
                    cmd = self.makeJobCommandEvent(job, pipe.getOriginator())
                    self.esys.publish(cmd, self.jobTopic)
                    self.bb.allocateNextJob(pipe.getOriginator())
                    out += 1

        return out

    def receiveReadyPipelines(self):
        """
        listen for pipelines ready to run and add them to the pipelinesReady
        queue
        """
        out = 0
        constraint = "status='job:ready'",
        pevent = self.esys.receiveEventWhere(self.jobTopic, constraint,
                                             self.initialWait)
        if not pevent:
            return 0

        while pevent:
            pitem = self.toPipelineQueueItem(pevent)
            if pitem:
                with self.bb.queues:
                    self.bb.queues.pipelinesReady.append(pitem)
                out += 1
            pevent = self.esys.receiveEventsWhere(self.jobTopic, constraint,
                                                  self.emptyWait)

        return out

    def toPipelineQueueItem(self, pevent):
        """
        convert a pipeline-ready event into a pipeline item.
        """
        self._notImplemented("toPipelineQueueItem")

class DataTriggeredJobOffice(_BaseJobOffice):
    """
    The behavior of this Job Office is controled completely on the description
    of data in the configuring policy file.  
    """
    
    def __init__(self, rootdir, log=None, policy=None):
        dpolf = DefaultPolicyFile("ctrl_sched",
                                  "DataTriggeredJobOffice_dict.paf",
                                  "policies")
        _BaseJobOffice.__init__(self, rootdir, log, policy, dpolf, True)

        # create a scheduler based on "schedule.className"
        self.scheduler = \
            DataTriggeredScheduler(self.bb, self.policy.getPolicy("schedule"),
                                   self.log)
                                   
    
    def processDataEvent(self, event):
        """
        process an event indicating that one or more datasets are available.
        @param event    the data event.  
        @return bool    true if the event was processed.
        """
        dsps = event.getProperties.getArray("dataset")
        for dsp in dsps:
            self.scheduler.processDataset(Dataset.fromPolicy(dsp))

        # wait until all events are processed
        #   self.scheduler.makeJobsAvailable()

    def findAvailableJobs(self):
        self.scheduler.makeJobsAvailable()

    
