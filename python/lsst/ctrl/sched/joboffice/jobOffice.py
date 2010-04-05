"""
jobOffice implementations
"""
from __future__ import with_statement

import lsst.pex.exceptions
from lsst.ctrl.sched.blackboard import Blackboard, Props
from lsst.ctrl.sched.blackboard import BasicBlackboardItem, PipelineItem, JobItem, DataProductItem
from lsst.ctrl.sched.base import _AbstractBase
from lsst.ctrl.sched import Dataset
from lsst.ctrl.events import EventSystem, EventReceiver, EventTransmitter, StatusEvent, CommandEvent
from lsst.pex.policy import Policy, DefaultPolicyFile, PolicyString, PAFWriter
from lsst.daf.base import PropertySet
from lsst.pex.logging import Log
from scheduler import DataTriggeredScheduler

import os, time, threading

def serializePolicy(policy):
    writer = PAFWriter()
    writer.write(policy)
    return writer.toString()

def unserializePolicy(policystr):
    return Policy.createPolicy(PolicyString(policystr))

class JobOffice(_AbstractBase, threading.Thread):
    """
    an abstract class that is responsible for using a blackboard to track
    the progress of running pipelines and sending them jobs as needed.
    """

    def __init__(self, persistDir, fromSubclass=False):
        """
        create the JobOffice
        """
        self._checkAbstract(fromSubclass, "JobOffice")
        threading.Thread.__init__(self)
        
        self.bb = Blackboard(persistDir)
        self.esys = EventSystem.getDefaultEventSystem()
        self.halt = False
        self.running = False
        self.originatorId = self.esys.createOriginatorId()

    def run(self, maxIterations=None):
        """
        continuously listen for events from pipelines and schedule jobs
        accordingly.  Unless maxIterations is set, this function will only
        exit when this job office has received stop signal or the JobOffice
        determines that work has been completed.
        @param maxIterations   loop through our JobOffice chores no more
                                  than this number of times.  If None
                                  (default), this function will not exit
                                  until the stop flag is set.
        """
        self._notImplemented("run")

    def stop(self):
        """
        set the stop flag to tell the JobOffice to stop running.
        """
        self.halt = True

class _BaseJobOffice(JobOffice):
    """
    an abstract JobOffice that provides much of the implementation for
    policy-configured behavior.  
    """

    def __init__(self, rootdir, log=None, policy=None, defPolicyFile=None,
                 brokerHost=None, brokerPort=None, fromSubclass=False):
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
        @param brokerHost     the machine where the event broker is running.
                                 If None (default), the host given in the
                                 policy is used.  This parameter is for
                                 carrying an override from the command line.
        @param brokerHost     the port to use to connect to the event broker.
                                 If None (default), the port given in the
                                 policy is used.  This parameter is for
                                 carrying an override from the command line.
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
                                       defPolicyFile.getRepositoryPath(),
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
        JobOffice.__init__(self, persistDir, True)

        # logger
        if not log:
            log = Log(Log.getDefaultLog(), self.name)
        self.log = log

        # initialize some data from policy
        self.initialWait = self.policy.get("listen.initialWait")
        self.emptyWait = self.policy.get("listen.emptyWait")
        self.dataTopics = self.policy.getArray("listen.dataReadyEvent")
        self.jobTopic = self.policy.get("listen.pipelineEvent")

        # initialize the event system
        self.jobReadyEvRcvr = self.dataEvRcvrs = None
        self.jobDoneEvRcvr = self.jobAcceptedEvRcvr = None
        if not brokerPort and self.policy.exists("listen.brokerHostPort"):
            brokerport = self.policy.get("listen.brokerHostPort")
        if not brokerHost and (not brokerPort or brokerPort > 0):
            brokerHost = self.policy.get("listen.brokerHostName")
        if brokerPort is None:
            self.dataEvRcvrs = []
            for topic in self.dataTopics:
                self.dataEvRcvrs.append(EventReceiver(brokerHost, topic))
            self.jobReadyEvRcvr = EventReceiver(brokerHost, self.jobTopic,
                                                "STATUS='job:ready'")
            self.jobDoneEvRcvr = EventReceiver(brokerHost, self.jobTopic,
                                               "STATUS='job:done'")
            self.jobAcceptedEvRcvr = EventReceiver(brokerHost, self.jobTopic,
                                                   "STATUS='job:accepted'")
            self.jobAssignEvTrx = EventTransmitter(brokerHost, self.jobTopic)
                                                   
        elif brokerPort > 0:
            self.dataEvRcvrs = []
            for topic in self.dataTopics:
                self.dataEvRcvrs.append(EventReceiver(brokerHost, brokerPort,
                                                      topic))
            self.jobReadyEvRcvr = EventReceiver(brokerHost, brokerPort,
                                                jobTopic,
                                                "STATUS='job:ready'")
            self.jobDoneEvRcvr = EventReceiver(brokerHost, brokerPort,
                                               jobTopic,
                                               "STATUS='job:done'")
            self.jobAcceptedEvRcvr = EventReceiver(brokerHost, brokerPort,
                                                   jobTopic,
                                                   "STATUS='job:accepted'")
            self.jobAssignEvTrx = EventTransmitter(brokerHost, brokerPort,
                                                   self.jobTopic)
            
    
    def run(self, maxIterations=None):
        """
        continuously listen for events from pipelines and schedule jobs
        accordingly.  Unless maxIterations is set, this function will only
        exit when this job office has received stop signal or the JobOffice
        determines that work has been completed.
        @param maxIterations   loop through our JobOffice chores no more
                                  than this number of times.  If None
                                  (default), this function will not exit
                                  until the stop flag is set.
        """
        i = 0
        max = maxIterations or 1
        while i < max:
            if self.halt:
                self.halt = False
                return
            
            # listen for completed Jobs
            self.processDoneJobs()

            # look for available data to add to dataAvailable queue
            self.processDataEvents()

            # look for possibleJobs that are ready to go
            self.findAvailableJobs()

            # listen for pipelines ready to run and give them jobs
            self.allocateJobs()

            if maxIterations is not None:
                i += 1

    def processDoneJobs(self):
        """
        listen for done events from pipelines in progress and update
        their state in the blackboard.
        @return int   the number of jobs found to be done
        """
        out = 0
        constraint = "status='job:done'",
        jevent = self.jobDoneEvRcvr.receiveStatusEvent(self.initialWait)
        if not jevent:
            return 0

        while jevent:
            if self.processJobDoneEvent(jevent):
                out += 1
            jevent = self.jobDoneEvRcvr.receiveStatusEvent(self.emptyWait)

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
            job = self.findByPipelineId(jevent.getOriginatorId())
            if not job:
                return False
            self.bb.markJobDone(job, jevent.getPropertySet().getAsBool("success"))
        return True

    def findByPipelineId(self, id):
        with self.bb.queues.jobsInProgress:
            for i in xrange(self.bb.queues.jobsInProgress.length()):
                job = self.bb.queues.jobsInProgress.get(i)
                if job.getPipelineId() == id:
                    return job
        return None

    def processDataEvents(self):
        """
        receive and process all data events currently available.
        @return int   the number of events processed
        """
        out = 0
        devent = self.receiveAnyDataEvent(self.initialWait)
        if not devent:
            return 0

        while devent:
            if self.processDataEvent(devent):
                out += 1
            devent = self.receiveAnyDataEvent(self.emptyWait)

        return out

    def receiveAnyDataEvent(self, timeout):
        if not self.dataEvRcvrs:
            return None
        if len(self.dataEvRcvrs) == 1:
            return self.dataEvRcvrs[0].receiveStatusEvent(timeout)
        
        now = t0 = time.time()
        eachtimeout = timeout/len(self.dataEvRcvrs)/10
        if eachtimeout == 0:  eachtimeout = 1
        while (now - t0 < timeout):
            # take a tenth of the total timeout time to go through list
            for rcvr in self.dataEvRcvrs:
                rcvr.receiveStatusEvent(eachtimeout)
            now = time.time()

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
            while not self.bb.queues.pipelinesReady.isEmpty() and \
                  not self.bb.queues.jobsAvailable.isEmpty():

                with self.bb.queues.pipelinesReady:
                    pipe = self.bb.queues.pipelinesReady.pop()
                    job = self.bb.queues.jobsAvailable.get(0)

                    # send a command to that pipeline
                    cmd = self.makeJobCommandEvent(job, pipe.getOriginator(),
                                                   pipe.getRunId())
                    self.jobAssignEvTrx.publishEvent(cmd)
                    self.bb.allocateNextJob(pipe.getOriginator())
                    out += 1

        return out

    def makeJobCommandEvent(self, job, pipeline, runid=""):
        """
        create a CommandEvent to send to a pipeline instructing it to
        commence working on the given job.
        """
        props = PropertySet()
        for ds in job.getDatasets():
            props.add("dataset", serializePolicy(ds.toPolicy()))
        props.set("STATUS", "job:process")
        props.set("name", job.getName())
        return CommandEvent(runid, self.originatorId, pipeline, props)
        

    def receiveReadyPipelines(self):
        """
        listen for pipelines ready to run and add them to the pipelinesReady
        queue
        """
        out = 0
        pevent = self.jobReadyEvRcvr.receiveStatusEvent(self.initialWait)
        if not pevent:
            return 0

        while pevent:
            pitem = self.toPipelineQueueItem(pevent)
            if pitem:
                with self.bb.queues:
                    self.bb.queues.pipelinesReady.append(pitem)
                out += 1
            pevent = self.jobReadyEvRcvr.receiveStatusEvent(self.emptyWait)

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
    
    def __init__(self, rootdir, log=None, policy=None, brokerHost=None,
                 brokerPort=None):
        dpolf = DefaultPolicyFile("ctrl_sched",
                                  "DataTriggeredJobOffice_dict.paf",
                                  "policies")
        _BaseJobOffice.__init__(self, rootdir, log, policy, dpolf,
                                brokerHost, brokerPort, True)

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
        out = 0
        dsps = event.getPropertySet().getArrayString("dataset")
        for dsp in dsps:
            if self.scheduler.processDataset(self.datasetFromProperty(dsp)):
                out += 1
        return out

        # wait until all events are processed
        #   self.scheduler.makeJobsAvailable()

    def datasetFromProperty(self, policystr):
        """
        convert the given string-encoded policy data into a Dataset
        @param policystr   the policy data written into a string in PAF format.
        """
        try:
            pol = unserializePolicy(policystr)
            return Dataset.fromPolicy(pol)
        except lsst.pex.exceptions.LsstCppException, ex:
            raise RuntimeError("Dataset encoding error: " + policystr)
            
    def toPipelineQueueItem(self, pevent):
        """
        convert a pipeline-ready event into a pipeline item.
        """
        props = { "ipid": pevent.getIPId(),
                  "status":  pevent.getStatus() }
        pipename = "unknown"
        if pevent.getPropertySet().exists("pipelineName"):
            pipename = pevent.getPropertySet().getString("pipelineName")
        pipe = PipelineItem.createItem(pipename, pevent.getRunId(),
                                       pevent.getOriginatorId(), props)
                          
        return pipe

    
        

    def findAvailableJobs(self):
        self.scheduler.makeJobsAvailable()

    
