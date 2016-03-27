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
tools and stages for pipelines that interact with the JobOffice scheduler
"""
from cStringIO import StringIO
import lsst.pex.harness.stage as harnessStage
from lsst.ctrl.sched import Dataset
import lsst.ctrl.sched.utils as utils
from lsst.ctrl.events import EventSystem, EventReceiver
from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.pex.logging import Log


class JobOfficeClient(object):
    """
    a component working on the behalf of a pipeline to receive processing
    assignments from a JobOffice scheduler.

    @see GetAJobStage, DataReadyStage, JobDoneStage
    """

    def __init__(self, runId, pipelineName, brokerHost,
                 originatorId=None, brokerPort=None):
        """
        @param runId         the Run ID for the pipeline
        @param pipelineName  the logical name of the pipeline
        @param topic         the topic to be used to communicate with
                                 the JobOffice
        """
        self.runId = runId
        self.name = pipelineName
        self.esys = EventSystem.getDefaultEventSystem()
        self.brokerhost = brokerHost
        self.brokerport = brokerPort

        if originatorId is None:
            originatorId = self.esys.createOriginatorId()
        self.origid = originatorId

    def getOriginatorId(self):
        return self.origid


class GetAJobClient(JobOfficeClient):
    """
    a component working on the behalf of a pipeline to receive processing
    assignments from a JobOffice scheduler.

    @see GetAJobStage
    """

    def __init__(self, runId, pipelineName, topic, brokerHost,
                 logger, brokerPort=None):
        """
        create the client
        @param runId         the Run ID for the pipeline
        @param pipelineName  the logical name of the pipeline
        @param topic         the topic to be used to communicate with
                                 the JobOffice
        @param brokerHost    the host where the event broker is running
        @param logger        the logger to use
        @param brokerPort    the port where the event broker is listening
        """
        JobOfficeClient.__init__(self, runId, pipelineName, brokerHost,
                                 brokerPort=brokerPort)

        self.sender = utils.EventSender(self.runId, topic, brokerHost,
                                        self.getOriginatorId())
        self.logger = logger
#        select = "RUNID='%s' and STATUS='job:assign'" \
#                 % (runId)
        select = "RUNID='%s' and DESTINATIONID=%d and STATUS='job:assign'" \
                 % (runId, self.getOriginatorId())
        if brokerPort:
            self.rcvr = EventReceiver(brokerHost, brokerPort, topic, select)
        else:
            self.rcvr = EventReceiver(brokerHost, topic, select)
        if self.logger:
            self.logger.log(Log.INFO-2, "selecting event with \"%s\" on %s" %
                            (select, topic))

    def getAssignment(self):
        """
        wait for an assignment (in the form of an event) from the JobOffice
        and return the info on the job to process.
        @return tuple   3 elements:  1) the jobIdentity dictionary,
                                     2) a list of the input datasets
                                     3) a list of the expected output datasets
        """
        event = self.rcvr.receiveCommandEvent()
        if not event:
            return (None, None, None)

        ps = event.getPropertySet()
        if self.logger:
            self.logger.log(Log.INFO-3,
                            "Received %s event for runid=%s" %
                            (event.getStatus(), event.getRunId()))
            self.logger.log(Log.INFO-3, "event properties: " + str(ps.names()))
        inputs = utils.unserializeDatasetList(
            ps.getArrayString("inputs"))
        outputs = utils.unserializeDatasetList(
            ps.getArrayString("outputs"))
        jobds = utils.unserializeDataset(ps.getString("identity"))
        jobid = jobds.ids is not None and jobds.ids.copy() or {}
        if jobds.type:
            jobid["type"] = jobds.type

        return (jobid, inputs, outputs)

    def tellReady(self):
        """
        tell the JobOffice that the pipeline is ready for an assignment.
        """
        self.sender.send(self.sender.createPipelineReadyEvent(self.name))


class DataReadyClient(JobOfficeClient):
    """
    a component working on the behalf of a pipeline to alert JobOffices
    about available datasets.

    @see DataReadyStage
    """

    def __init__(self, runId, pipelineName, topic, brokerHost,
                 datasetType=None, reportAllPossible=True, brokerPort=None):
        """
        create the client
        @param runId         the Run ID for the pipeline
        @param pipelineName  the logical name of the pipeline
        @param topic         the topic to be used to communicate with
                                 the JobOffice
        @param brokerHost    the host where the event broker is running
        @param datasetType   the dataset type to restrict notifications to.
                                 If None, all types will be handled.
        @param reportAllPossible  if False, the data ready event will only 
                             be issued for those datasets that have thus
                             far been completed.
        """
        JobOfficeClient.__init__(self, runId, pipelineName, brokerHost,
                                 brokerPort=brokerPort)

        self.datasetType = datasetType
        self.reportAllPossible = reportAllPossible

        self.dataSender = utils.EventSender(self.runId, topic, brokerHost,
                                            self.getOriginatorId(), brokerPort)

    def tellDataReady(self, possible, completed=None, defSuccess=False):
        """
        alert JobOffices that one or more datasets are ready.
        @param possible    a single or list of possible datasets to announce
        @param completed   the list of datasets that have actually been
                           successfully created.
        @param defSuccess  the default validity flag to set if the possible
                             dataset is not among the completed ones.
        @return list of Datasets:  the datasets that were not announced
        """
        if not isinstance(possible, list):
            possible = [possible]
        if completed is None:
            completed = []
        elif not isinstance(completed, list):
            completed = [completed]

        remain = []
        report = []
        fullsuccess = True
        while len(possible) > 0:
            ds = possible.pop(0)
            if self.datasetType and self.datasetType != ds.type:
                # only notify on the dataset type of interest
                remain.append(ds)
                continue

            ds.valid = ds in completed
            if self.reportAllPossible and not ds.valid:
                fullsuccess = False
            if self.reportAllPossible or ds.valid:
                report.append(ds)
            else:
                remain.append(ds)

        self.dataSender.send(
            self.dataSender.createDatasetEvent(self.name, report, fullsuccess))
        return remain


class JobDoneClient(JobOfficeClient):
    """
    a component working on the behalf of a pipeline to alert JobOffices
    that a job is finished.  

    @see JobDoneStage
    """

    def __init__(self, runId, pipelineName, topic, brokerHost, brokerPort=None):
        """
        create the client
        @param runId         the Run ID for the pipeline
        @param pipelineName  the logical name of the pipeline
        @param topic         the topic to be used to communicate with
                                 the JobOffice
        """
        JobOfficeClient.__init__(self, runId, pipelineName,
                                 brokerHost, brokerPort=brokerPort)

        self.jobSender = utils.EventSender(self.runId, topic, brokerHost,
                                           self.getOriginatorId(), brokerPort)

    def tellDone(self, success, originatorId):
        """
        alert the JobOffice that assigned job is done
        """
        self.jobSender.send(self.jobSender.createJobDoneEvent(self.name,
                                                              success,
                                                              originatorId))


class _GetAJobComp(object):

    def setup(self):
        deffile = DefaultPolicyFile("ctrl_sched", "GetAJob_dict.paf", "policies")
        defpol = Policy.createPolicy(deffile, deffile.getRepositoryPath())

        if not hasattr(self, "policy") or not self.policy:
            self.policy = Policy()
        self.policy.mergeDefaults(defpol.getDictionary())

        self.jobid = None
        self.tagLogger(None)

#        self.mode = self.policy.getString("mode")
#        if self.mode not in "parallel serial":
#            raise RuntimeError("Stage %s: Unsupported mode: %s" %
#                               (self.getName(), self.mode))

        self.clipboardKeys = {}
        self.clipboardKeys["jobIdentity"] = \
            self.policy.getString("outputKeys.jobIdentity")
        self.clipboardKeys["inputDatasets"] = \
            self.policy.getString("outputKeys.inputDatasets")
        self.clipboardKeys["outputDatasets"] = \
            self.policy.getString("outputKeys.outputDatasets")
        self.clipboardKeys["completedDatasets"] = \
            self.policy.getString("outputKeys.completedDatasets")
        self.log.log(Log.INFO-1, "clipboard keys: " + str(self.clipboardKeys))

        topic = self.policy.getString("pipelineEvent")
        self.client = GetAJobClient(self.getRun(), self.getName(), topic,
                                    self.getEventBrokerHost(), self.log)
        self.log.log(Log.INFO-1,
                     "Using OriginatorId = %d" % self.client.getOriginatorId())

    def setAssignment(self, clipboard):
        clipboard.put("originatorId", self.client.getOriginatorId())
        self.client.tellReady()
        self.log.log(Log.INFO-2, "Told JobOffice, I'm ready!")
        jobid, inputs, outputs = self.client.getAssignment()
        if jobid is None:
            raise RuntimeError("empty assignment from JobOffice (event timed out?)")

        self.log.log(Log.INFO-2, "Received assignment for pipeline #" +
                     str(clipboard.get("originatorId")))

        # ids is a dictionary
        allZero = 1
        for inputKey, inputValue in inputs[0].ids.iteritems():
            # self.log.log(Log.INFO, "key  " + str(inputKey) + " value-" + str(inputValue) + "----")
            if str(inputValue) != "0":
                allZero = 0

        if allZero == 1:
            self.log.log(Log.INFO, "All of the attributes are zero, denoting noMoreDatasets ")
            clipboard.put("noMoreDatasets", allZero)

        clipboard.put(self.clipboardKeys["inputDatasets"], inputs)
        clipboard.put(self.clipboardKeys["outputDatasets"], outputs)
        clipboard.put(self.clipboardKeys["completedDatasets"], [])
        clipboard.put(self.clipboardKeys["jobIdentity"], jobid)
        self.tagLogger(jobid.copy())
        self.log.log(Log.INFO, "Processing job: " + self.jobidStr)

    def tagLogger(self, jobid):
        idstr = []
        if not jobid:
            # clear out the previous info
            if self.jobid:
                for key in self.jobid.keys():
                    self._resetLogJobId(self.jobid, key)
            else:
                self.jobid = {}
            jobid = self.jobid
            idstr.append("unknown")
        else:
            self.jobid = jobid
            for key in self.jobid.keys():
                idstr.append("%s=%s" % (key, str(jobid[key])))

# this does not work as intended (i.e. properties do not get into the
# intended loggers).  Until this is made possible by pex_logging, we will
# just add these properties to our own local logger.
#
#        root = Log.getDefaultLog()
#
        root = self.log

        self.jobidStr = " ".join(idstr)
        self.log.setPreamblePropertyString("JobId", self.jobidStr)
        root.setPreamblePropertyString("JobId", self.jobidStr)

        for key in jobid.keys():
            self._setLogJobIdValue(self.log, jobid, key)
            self._setLogJobIdValue(root, jobid, key)

    def _resetLogJobId(self, jobid, key):
        if jobid.has_key(key):
            if isinstance(jobid[key], int):
                jobid[key] = -1
            elif isinstance(jobid[key], long):
                jobid[key] = -1L
            elif isinstance(jobid[key], float):
                jobid[key] = 0.0
            elif isinstance(jobid[key], bool):
                jobid[key] = False
            else:
                jobid[key] = ""

    def _setLogJobIdValue(self, log, jobid, key):
        if jobid.has_key(key):
            name = "JobId_" + key
            if isinstance(jobid[key], int):
                log.setPreamblePropertyInt(name, jobid[key])
            elif isinstance(jobid[key], long):
                log.setPreamblePropertyLong(name, jobid[key])
            elif isinstance(jobid[key], float):
                log.setPreamblePropertyDouble(name, jobid[key])
            elif isinstance(jobid[key], bool):
                log.setPreamblePropertyBool(name, jobid[key])
            else:
                log.setPreamblePropertyString(name, jobid[key])


class GetAJobParallelProcessing(_GetAJobComp, harnessStage.ParallelProcessing):
    """
    Stage implementation that gets a job assignment for processing by
    the parallel Slice.
    """

    def process(self, clipboard):
        """
        get the job assignment and post it to the clipboard.
        """
        self.tagLogger(None)
        self.setAssignment(clipboard)


class GetAJobSerialProcessing(_GetAJobComp, harnessStage.SerialProcessing):
    """
    Stage implementation that gets a job assignment for processing by
    the master Pipeline thread.
    """

    def preprocess(self, clipboard):
        """
        get the job assignment and post it to the clipboard.
        """
        self.tagLogger(None)
        self.setAssignment(clipboard)


class _DataReadyComp(object):

    def setup(self, policyDict="DataReady_dict.paf"):
        deffile = DefaultPolicyFile("ctrl_sched", policyDict, "policies")
        defpol = Policy.createPolicy(deffile, deffile.getRepositoryPath())

        if not hasattr(self, "policy") or not self.policy:
            self.policy = Policy()
        self.policy.mergeDefaults(defpol.getDictionary())

#        self.mode = self.policy.getString("mode")
#        if self.mode not in "parallel serial":
#            raise RuntimeError("Stage %s: Unsupported mode: %s" %
#                               (self.getName(), self.mode))

        self.clipboardKeys = {}
        self.clipboardKeys["completedDatasets"] = \
            self.policy.getString("inputKeys.completedDatasets")
        self.clipboardKeys["possibleDatasets"] = \
            self.policy.getString("inputKeys.possibleDatasets")

        self.dataclients = []
        clpols = []
        if self.policy.exists("datasets"):
            clpols = self.policy.getPolicyArray("datasets")
        for pol in clpols:
            dstype = None
            if pol.exists("datasetType"):
                dstype = pol.getString("datasetType")
            topic = pol.getString("dataReadyEvent")
            reportAll = pol.getBool("reportAllPossible")
            client = DataReadyClient(self.getRun(), self.getName(), topic,
                                     self.getEventBrokerHost(), dstype,
                                     reportAll)
            self.dataclients.append(client)

    def tellDataReady(self, clipboard):
        """
        send an event reporting on the output datasets that have been
        attempted by this pipeline.
        @param clipboard     the pipeline clipboard containing the output
                               datasets
        """
        completed = clipboard.get(self.clipboardKeys["completedDatasets"])
        possible = clipboard.get(self.clipboardKeys["possibleDatasets"])

        for client in self.dataclients:
            if not possible:
                break
            self.log.log(Log.DEBUG, "completed: " + str(completed))
            possible = client.tellDataReady(possible, completed)

        # update the possible list for the ones we have not reported
        # on yet.
        clipboard.put(self.clipboardKeys["possibleDatasets"], possible)


class DataReadyParallelProcessing(_DataReadyComp, harnessStage.ParallelProcessing):
    """
    Stage implementation that reports on newly available datasets via the
    Slice threads.
    """

    def process(self, clipboard):
        """
        examine the clipboard for the list of persisted datasets and
        announce their availability to JobOffices via an event.
        """
        self.tellDataReady(clipboard)


class DataReadySerialProcessing(_DataReadyComp, harnessStage.SerialProcessing):
    """
    Stage implementation that reports on newly available datasets via the
    master Pipeline thread.
    """

    def preprocess(self, clipboard):
        """
        examine the clipboard for the list of persisted datasets and
        announce their availability to JobOffices via an event.
        """
        self.tellDataReady(clipboard)


class _JobDoneComp(_DataReadyComp):

    def setup(self):
        _DataReadyComp.setup(self, "JobDone_dict.paf")

        self.jobsuccess = self.policy.getBool("jobSuccess")

        topic = self.policy.getString("pipelineEvent")
        self.jobclient = JobDoneClient(self.getRun(), self.getName(), topic,
                                       self.getEventBrokerHost())

    def tellJobDone(self, clipboard=None):
        """
        alert the JobOffice that this pipeline completed its job.  This
        will also alert about ready datasets.
        """
        origid = self.jobclient.getOriginatorId()
        if clipboard:
            if clipboard.has_key("originatorId"):
                origid = clipboard.get("originatorId")
            else:
                self.log.log(Log.WARN, "OriginatorId not found on clipboard")
                print "DEBUG: clipboard keys:", str(clipboard.keys())
            if len(self.dataclients) > 0:
                self.log.log(Log.INFO-5, "reporting the completed files")
                self.tellDataReady(clipboard)
        self.jobclient.tellDone(self.jobsuccess, origid)


class JobDoneParallelProcessing(_JobDoneComp, harnessStage.ParallelProcessing):
    """
    Stage implementation that reports on newly available datasets via the
    Slice threads.
    """

    def process(self, clipboard):
        """
        examine the clipboard for the list of persisted datasets and
        announce their availability to JobOffices via an event.
        """
        self.tellJobDone(clipboard)


class JobDoneSerialProcessing(_JobDoneComp, harnessStage.SerialProcessing):
    """
    Stage implementation that reports on newly available datasets via the
    master Pipeline thread.
    """

    def preprocess(self, clipboard):
        """
        examine the clipboard for the list of persisted datasets and
        announce their availability to JobOffices via an event.
        """
        self.tellJobDone(clipboard)


class GetAJobFromMasterStage(harnessStage.Stage):
    """
    Stage implementation that gets a job assignment for processing by
    the master Pipeline thread.
    """
    serialClass = GetAJobSerialProcessing


class GetAJobFromSliceStage(harnessStage.Stage):
    """
    Stage implementation that gets a job assignment for processing by
    the Slice thread.
    """
    parallelClass = GetAJobParallelProcessing


class JobDoneFromMasterStage(harnessStage.Stage):
    """
    Stage implementation that reports on newly available datasets.
    """
    serialClass = JobDoneSerialProcessing


class JobDoneFromSliceStage(harnessStage.Stage):
    """
    Stage implementation that reports on newly available datasets.
    """
    parallelClass = JobDoneParallelProcessing


class DataReadyFromMasterStage(harnessStage.Stage):
    """
    Stage implementation that reports on newly available datasets via the
    master Pipeline thread.
    """
    serialClass = DataReadySerialProcessing


class DataReadyFromSliceStage(harnessStage.Stage):
    """
    Stage implementation that reports on newly available datasets via the
    master Pipeline thread.
    """
    parallelClass = DataReadyParallelProcessing

