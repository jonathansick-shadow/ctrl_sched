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
from lsst.pex.logging import Log, BlockTimingLog
from scheduler import DataTriggeredScheduler
from lsst.ctrl.sched.utils import serializePolicy, unserializePolicy

import os, time, threading
import traceback as tb

VERB2 = -2
VERB3 = -3
TRACE = Log.DEBUG

class JobOffice(_AbstractBase, threading.Thread):
    """
    an abstract class that is responsible for using a blackboard to track
    the progress of running pipelines and sending them jobs as needed.
    """

    def __init__(self, persistDir, logger=None, runId=None,
                 fromSubclass=False):
        """
        create the JobOffice
        @param persistDir   the directory where the blackboard should be
                              persisted
        @param logger       the logger to use.  If not given, the default
                              will be used.
        @param runId        the run ID to restrict our attention to.  If
                              not None, incoming events will be restricted
                              to the given runId.
        """
        self._checkAbstract(fromSubclass, "JobOffice")
        threading.Thread.__init__(self, name="JobOffice")
        self.setDaemon(False)

        self.bb = Blackboard(persistDir)
        self.esys = EventSystem.getDefaultEventSystem()
        self.runId = runId
        self.brokerHost = None
        self.brokerPort = None
        self.originatorId = self.esys.createOriginatorId()

        if logger is None:
            logger = Log.getDefaultLog()
        self.log = logger   # override this in sub-class

        self.halt = False
        self.finalDatasetSent = False
        self.stopTopic = "JobOfficeStop"
        self.stopThread = None
        self.exc = None

    def managePipelines(self, maxIterations=None):
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

    def run(self):
        """
        execute managePipelines in the context of a separate thread.
        """
        self._checkAbstract(self.brokerHost, "JobOffice")
        self.stopThread = self._StopThread(self, self.stopTopic, self.runId,
                                           self.brokerHost, self.brokerPort)
        self.stopThread.start()
        try:
            self.log.log(Log.INFO, "starting.")
            self.managePipelines()
        except Exception, ex:
            tb.print_exc()
            self.exc = ex
        self.log.log(Log.INFO, "job office done.")

    def stop(self):
        """
        set the stop flag to tell the JobOffice to stop running.
        """
        self.halt = True

    class _StopThread(threading.Thread):

        def __init__(self, joboffice, stopTopic, runId, brokerHost, 
                     brokerPort=None, waittime=60):

            threading.Thread.__init__(self, name=joboffice.getName()+".stop")
            self.setDaemon(True)
            
            self.jo = joboffice
            self.timeout = waittime

            self.log = Log(self.jo.log, "stop")

            selector = ""
            if runId:  selector = "RUNID='%s'" % runId
                
            if brokerPort:
                self.rcvr = EventReceiver(brokerHost, brokerPort, stopTopic,
                                          selector)
            else:
                self.rcvr = EventReceiver(brokerHost, stopTopic, selector)
                
        def run(self):
            while True:
                event = self.rcvr.receiveEvent(self.timeout)
                if event:
                    self.log.log(Log.INFO-1, "received stop event; " +
                                 "shutting down JobOffice thread")
                    self.jo.stop()
                if self.jo.halt:
                    return

    classLookup = { }

class _BaseJobOffice(JobOffice):
    """
    an abstract JobOffice that provides much of the implementation for
    policy-configured behavior.  
    """

    def __init__(self, rootdir, policy=None, defPolicyFile=None, log=None, 
                 runId=None, brokerHost=None, brokerPort=None, forDaemon=False,
                 fromSubclass=False):
        """
        create the JobOffice
        @param rootdir        the root directory where job offices may set up
                                 its blackboard data.  This JobOffice will
                                 create a subdirectory here for its data with
                                 a name set by the "name" policy paramter.
        @param policy         the policy to use to configure this JobOffice
        @param defPolicyFile  the DefaultPolicyFile to use for defaults.  If
                                 this points to a dictionary, a policy
                                 validation will be done.  If None, an
                                 internally identified default policy file
                                 will be used.
        @param log            a logger to use; if None, the default logger will
                                 will be used.  A child log will be created.
        @param runId          the run ID to restrict our attention to.  If
                                 not None, incoming events will be restricted
                                 to the given runId.
        @param brokerHost     the machine where the event broker is running.
                                 If None (default), the host given in the
                                 policy is used.  This parameter is for
                                 carrying an override from the command line.
        @param brokerPort     the port to use to connect to the event broker.
                                 If None (default), the port given in the
                                 policy is used.  This parameter is for
                                 carrying an override from the command line.
        @param forDaemon      if true, the creation of the Event channels will
                                 be delayed until run() is called.  This allows
                                 the caller to fork as needed without losing
                                 the connections with the event broker.  This
                                 also means that some public functions (other
                                 than run() and start()) will not work
                                 properly until ensureReady() is called.  
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
        if not os.path.exists(persistDir):
            os.makedirs(persistDir)
        JobOffice.__init__(self, persistDir, log, runId, True)
        self.setName(self.name)

        # logger
        self.log = Log(self.log, self.name)

        # initialize some data from policy
        self.initialWait = self.policy.get("listen.initialWait")
        self.emptyWait = self.policy.get("listen.emptyWait")
        self.dataTopics = self.policy.getArray("listen.dataReadyEvent")
        self.jobTopic = self.policy.get("listen.pipelineEvent")
        self.stopTopic = self.policy.get("listen.stopEvent")
        self.jobOfficeTopic = self.policy.get("listen.jobOfficeEvent")

        self.highWatermark = self.policy.get("listen.highWatermark")

        # initialize the event system
        self.jobReadyEvRcvr = self.dataEvRcvrs = None
        self.jobDoneEvRcvr = self.jobAcceptedEvRcvr = None
        self.brokerHost = brokerHost
        self.brokerPort = brokerPort
        if not self.brokerPort and self.policy.exists("listen.brokerHostPort"):
            self.brokerPort = self.policy.get("listen.brokerHostPort")
        if not self.brokerHost and (not self.brokerPort or self.brokerPort > 0):
            self.brokerHost = self.policy.get("listen.brokerHostName")

        self.dataEvRcvrs       = None
        self.jobReadyEvRcvr    = None
        self.jobDoneEvRcvr     = None
        self.jobAcceptedEvRcvr = None
        self.jobAssignEvTrx    = None
        self.jobOfficeRcvr     = None
        if not forDaemon:
            self._setEventPipes()

    def ensureReady(self):
        if self.dataEvRcvrs is None:
            self._setEventPipes()

    def _setEventPipes(self):
        select = ""
        if self.runId:
            select = "RUNID='%s'" % self.runId
            
        if self.brokerPort is None:
            self.dataEvRcvrs = []
            for topic in self.dataTopics:
                self.dataEvRcvrs.append(EventReceiver(self.brokerHost, topic,
                                                      select))

            self.jobReadyEvRcvr = EventReceiver(self.brokerHost, self.jobTopic,
                                                select+"and STATUS='job:ready'")
            self.jobDoneEvRcvr = EventReceiver(self.brokerHost, self.jobTopic,
                                               select+"and STATUS='job:done'")
            self.jobAcceptedEvRcvr = EventReceiver(self.brokerHost,
                                                   self.jobTopic,
                                               select+"and STATUS='job:accepted'")
            self.jobAssignEvTrx = EventTransmitter(self.brokerHost,
                                                   self.jobTopic)
            self.jobOfficeRcvr = EventReceiver(self.brokerHost,
                                                 self.jobOfficeTopic,
                                                 select)


                                                   
        elif self.brokerPort > 0:
            self.dataEvRcvrs = []
            for topic in self.dataTopics:
                self.dataEvRcvrs.append(EventReceiver(self.brokerHost,
                                                      topic, select, self.brokerPort))

            self.jobReadyEvRcvr = EventReceiver(self.brokerHost,
                                                jobTopic,
                                                select+"and STATUS='job:ready'",
                                                self.brokerPort)
            self.jobDoneEvRcvr = EventReceiver(self.brokerHost,
                                               jobTopic,
                                               select+"and STATUS='job:done'",
                                               self.brokerPort)
            self.jobAcceptedEvRcvr = EventReceiver(self.brokerHost,
                                                   jobTopic,
                                                   select+"and STATUS='job:accepted'",
                                                   self.brokerPort)
            self.jobAssignEvTrx = EventTransmitter(self.brokerHost,
                                                   self.jobTopic, 
                                                   self.brokerPort)

            self.jobOfficeRcvr = EventReceiver(self.brokerHost,
                                                 self.jobOfficeTopic,
                                                 select, 
                                                 self.brokerPort);

            
    
    def managePipelines(self, maxIterations=None):
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
        self.ensureReady()
        
        i = 0
        max = maxIterations or 1
        while i < max:
            if self.halt:
                self.halt = False
                self.log.log(Log.INFO, "Stop requested; shutting down.")
                return

            trace = self._trace("loop", VERB3)

            self.processJobOfficeEvents()
            
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

            # look to see if all jobs are complete.
            self.observeStatusOfJobs()

            trace.done()

    def observeStatusOfJobs(self):
        trace = self._trace("observeStatusOfJobs")
        if self.finalDatasetSent == True:
            with self.bb.queues:
                 done = self.bb.queues.jobsInProgress.isEmpty() and self.bb.queues.jobsAvailable.isEmpty()
                 self.log.log(Log.DEBUG, "observe: jobsInProgress.length() = "+ str(self.bb.queues.jobsInProgress.length()))
                 self.log.log(Log.DEBUG, "observe: jobsInProgress.isEmpty() = "+ str(self.bb.queues.jobsInProgress.isEmpty()))
                 self.log.log(Log.DEBUG, "observe: jobsInAvailable.length() = "+ str(self.bb.queues.jobsAvailable.length()))
                 self.log.log(Log.DEBUG, "observe: jobsInAvailable.isEmpty() = "+ str(self.bb.queues.jobsAvailable.isEmpty()))
                 if done:
                    self.log.log(Log.DEBUG, "observe: done!")
                    self.halt = True
                 else:
                    self.log.log(Log.DEBUG, "observe: NOT done!")
        trace.done()

    def processJobOfficeEvents(self):
        trace = self._trace("processJobOfficeEvents")
        jevent = self.jobOfficeRcvr.receiveStatusEvent(self.emptyWait)
        if not jevent:
            trace.done()
            return
        self.finalDatasetSent = True
        trace.done()
        return

    def processDoneJobs(self):
        """
        listen for done events from pipelines in progress and update
        their state in the blackboard.
        @return int   the number of jobs found to be done
        """
        trace = self._trace("processDoneJobs")
        out = 0
        jevent = self.jobDoneEvRcvr.receiveStatusEvent(self.initialWait)
            
        if not jevent:
            trace.done()
            return 0

        while jevent:
            if self.processJobDoneEvent(jevent):
                out += 1
            self._logJobDone(jevent)
            jevent = self.jobDoneEvRcvr.receiveStatusEvent(self.emptyWait)

        trace.done()
        return out

    def _logJobDone(self, jobevent):
        try: 
            self._debug("%s: %s on %s finised %s",
                        (jobevent.getStatus(),
                         jobevent.getPropertySet().getString("pipelineName"),
                         jobevent.getHostId(),
                         (jobevent.getPropertySet().getBool("success") and
                          "successfully") or "with failure"))
        except Exception, ex:
            self.log.log(Log.WARN, "logging error on " + jobevent.getStatus()
                         + ": " + str(ex))

    def _debug(self, msg, data=None):
        self._log(VERB2, msg, data)
    def _inform(self, msg, data=None):
        self._log(Log.INFO, msg, data)

    def _log(self, level, msg, data=None):
        if data is None:
            self.log.log(level, msg)
        else:
            try:
                self.log.log(level, msg % data)
            except Exception, ex:
                self.log.log(Log.WARN, "Trouble logging: " + msg + " % " + str(data))

    def _trace(self, where, lev=TRACE):
        out = BlockTimingLog(self.log, where, lev)
        out.start()
        return out
            
    def processJobDoneEvent(self, jevent):
        """
        process a job-done event.  If the event matches a job in the 
        jobsInProgress queue, the job will be moved to the jobsDone queue.
        @param jevent   the job event to process
        """
        if not isinstance(jevent, StatusEvent):
            self.log.log(Log.WARN, "Job event has wrong type: " +
                         str(type(jevent)))
            return False
        
        with self.bb:
            job = self.findByPipelineId(jevent.getOriginatorId())
            if not job:
                self.log.log(Log.WARN, "Failed to find job for event: " + str(jevent.getOriginatorId()))
                return False
            success = jevent.getPropertySet().getAsBool("success")

            # if the job failed, decrement the retry counter, and check to see if 
            # the job can be retried.  If it can, reschedule it.  If not, mark it as done.
            if success == False:
                job.decrementRetries()
                if job.canRetry() == True:
                    self.bb.rescheduleJob(job)
                    return True
            self.bb.markJobDone(job, success)
        return True

    def findByPipelineId(self, id):
        with self.bb.queues.jobsInProgress:
            self.log.log(Log.DEBUG, "findByPipelineId: jobsInProgress.length() = "+ str(self.bb.queues.jobsInProgress.length()))
            self.log.log(Log.DEBUG, "findByPipelineId: looking up id= "+ str(id))
            for i in xrange(self.bb.queues.jobsInProgress.length()):
                job = self.bb.queues.jobsInProgress.get(i)
                if job.getPipelineId() == id:
                    return job
                # print "DEBUG:", "%s != %s" % (job.getPipelineId(), id)
		self.log.log(Log.WARN, "findByPipelineId: %s != %s" % (str(job.getPipelineId()), str(id)))
        return None

    def processDataEvents(self):
        """
        receive and process all data events currently available.
        @return int   the number of events processed
        """
        trace = self._trace("processDataEvents")
        out = 0
        devent = self.receiveAnyDataEvent(self.initialWait)
        if not devent:
            trace.done()
            return 0

        watermark = 0
        while devent:
            self._logDataEvent(devent)
            if self.processDataEvent(devent):
                out += 1
            watermark += 1
            if watermark >= self.highWatermark:
                trace.done()
                return out
            devent = self.receiveAnyDataEvent(self.emptyWait)

        trace.done()
        return out

    def _logDataEvent(self, dataevent):
        try: 
            self._log(VERB3, "%s dataset(s) ready", dataevent.getTopic())
        except Exception, ex:
            self.log.log(Log.WARN, "logging error on " + dataevent.getStatus()
                         + ": " + str(ex))
                         
        
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
        trace = self._trace("allocateJobs")
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

        trace.done()
        return out

    def makeJobCommandEvent(self, job, pipeline, runId=""):
        """
        create a CommandEvent to send to a pipeline instructing it to
        commence working on the given job.
        """
        props = PropertySet()
        for ds in job.getInputDatasets():
            props.add("inputs", serializePolicy(ds.toPolicy()))
        for ds in job.getOutputDatasets():
            props.add("outputs", serializePolicy(ds.toPolicy()))
        props.set("identity", serializePolicy(job.getJobIdentity().toPolicy()))
        props.set("STATUS", "job:assign")
        props.set("name", job.getName())
        return CommandEvent(runId, self.originatorId, pipeline, props)
        

    def receiveReadyPipelines(self):
        """
        listen for pipelines ready to run and add them to the pipelinesReady
        queue
        """
        trace = self._trace("receiveReadyPipelines")
        out = 0
        pevent = self.jobReadyEvRcvr.receiveStatusEvent(self.initialWait)
        if not pevent:
            trace.done()
            return 0

        while pevent:
            self._logReadyPipe(pevent)
            pitem = self.toPipelineQueueItem(pevent)
            if pitem:
                with self.bb.queues:
                    self.bb.queues.pipelinesReady.append(pitem)
                out += 1
            pevent = self.jobReadyEvRcvr.receiveStatusEvent(self.emptyWait)

        trace.done()
        return out

    def _logReadyPipe(self, pipeevent):
        try:
            self._debug("%s: %s on %s is ready",
                        (pipeevent.getStatus(),
                         pipeevent.getPropertySet().getString("pipelineName"),
                         pipeevent.getHostId()))
        except Exception, ex:
            self.log.log(Log.WARN, "logging error on " + pipeevent.getStatus()
                         + ": " + str(ex))
            

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
    
    def __init__(self, rootdir, policy=None, log=None, runId=None, 
                 brokerHost=None, brokerPort=None, forDaemon=False):
        """
        create a JobOffice that is triggered by the appearence of data
        that matched filters specified in the policy file.
        @param rootdir        the root directory where job offices may set up
                                 its blackboard data.  This JobOffice will
                                 create a subdirectory here for its data with
                                 a name set by the "name" policy paramter.
        @param policy         the policy to use to configure this JobOffice
        @param defPolicyFile  the DefaultPolicyFile to use for defaults.  If
                                 this points to a dictionary, a policy
                                 validation will be done.  If None, an
                                 internally identified default policy file
                                 will be used.
        @param log            a logger to use; if None, the default logger will
                                 will be used.  A child log will be created.
        @param runId          the run ID to restrict our attention to.  If
                                 not None, incoming events will be restricted
                                 to the given runId.
        @param brokerHost     the machine where the event broker is running.
                                 If None (default), the host given in the
                                 policy is used.  This parameter is for
                                 carrying an override from the command line.
        @param brokerHost     the port to use to connect to the event broker.
                                 If None (default), the port given in the
                                 policy is used.  This parameter is for
                                 carrying an override from the command line.
        @param forDaemon      if true, the creation of the Event channels will
                                 be delayed until run() is called.  This allows
                                 the caller to fork as needed without losing
                                 the connections with the event broker.  This
                                 also means that some public functions (other
                                 than run() and start()) will not work
                                 properly until ensureReady() is called.  
        """
        dpolf = DefaultPolicyFile("ctrl_sched",
                                  "DataTriggeredJobOffice_dict.paf",
                                  "policies")
        _BaseJobOffice.__init__(self, rootdir, policy, dpolf, log, runId, 
                                brokerHost, brokerPort, forDaemon, True)

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
            ds = self.datasetFromProperty(dsp)
            self._logDataReady(ds)
            if self.scheduler.processDataset(ds):
                out += 1
        return out

        # wait until all events are processed
        #   self.scheduler.makeJobsAvailable()

    def _logDataReady(self, dataset):
        self._debug("Dataset %s is ready", dataset.toString())

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
        trace = self._trace("findAvailableJobs")
        self.scheduler.makeJobsAvailable()
        trace.done()

JobOffice.classLookup["DataTriggered"] = DataTriggeredJobOffice
JobOffice.classLookup["DataTriggeredJobOffice"] = DataTriggeredJobOffice
    
__all__ = "JobOffice DataTriggeredJobOffice".split()

