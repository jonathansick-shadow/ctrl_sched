"""
tools and stages for pipelines that interact with the JobOffice scheduler
"""
import lsst.pex.harness.stage as harnessStage
from dataset import Dataset
import lsst.ctrl.sched.utils as utils
from lsst.ctrl.events import EventSystem, EventReceiver
from lsst.pex.policy import Policy, DefaultPolicyFile


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
        self.name = pipelineName
        self.esys = EventSystem.getDefaultEventSystem()
        self.brokerhost = brokerHost
        self.brokerport = brokerPort

        if originatorId is None:
            originatorId = self.esys.createOriginatorId()
        self.origid = originatorId

    def getOriginatorId():
        return self.origid

class GetAJobClient(JobOfficeClient):
    """
    a component working on the behalf of a pipeline to receive processing
    assignments from a JobOffice scheduler.

    @see GetAJobStage
    """

    def __init__(self, runId, pipelineName, topic, brokerHost,
                 brokerPort=None):
        """
        create the client
        @param runId         the Run ID for the pipeline
        @param pipelineName  the logical name of the pipeline
        @param topic         the topic to be used to communicate with
                                 the JobOffice
        """
        JobOfficeClient.__init__(self, runId, pipelineName, brokerHost,
                                 brokerPort=brokerPort)

        self.sender = utils.EventSender(self.runid, topic, brokerHost)
        select = "RUNID='%s' and STATUS='%s:%s'" % \
                 (runId, topic, "ready")
        if brokerPort:
            self.rcvr = EventReceiver(brokerHost, brokerPort, topic, select)
        else:
            self.rcvr = EventReceiver(brokerHost, topic, select)

    def getAssignment(self):
        """
        wait for an assignment (in the form of an event) from the JobOffice
        and return the info on the job to process.
        @return tuple   3 elements:  1) the jobIdentity dictionary,
                                     2) a list of the input datasets
                                     3) a list of the expected output datasets
        """
        event = self.rcvr.receiveStatusEvent()
        if not event:
            return None

        ps = event.getPropertySet()
        inputs = utils.unserializeDatasetList(
                                       ps.getArrayString("inputDatasets"))
        outputs = utils.unserializeDatasetList(
                                       ps.getArrayString("outputDatasets"))
        jobds = utils.unserializeDataset(ps.getString("identity"))
        jobid = jobs.ids.copy()
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
        
        self.dataSender = utils.EventSender(self.runid, topic, brokerHost,
                                            brokerPort)


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
            self.sender.createDatasetEvent(self.name, report, fullsuccess))
        return remain


class JobDoneClient(JobOfficeClient):
    """
    a component working on the behalf of a pipeline to alert JobOffices
    that a job is finished.  

    @see JobDoneStage
    """

    def __init__(self, runId, pipelineName, topic, brokerHost,brokerPort=None):
        """
        create the client
        @param runId         the Run ID for the pipeline
        @param pipelineName  the logical name of the pipeline
        @param topic         the topic to be used to communicate with
                                 the JobOffice
        """
        JobOfficeClient.__init__(self, runId, pipelineName, 
                                 brokerHost, brokerPort=brokerPort)
                                 
        self.jobSender = utils.EventSender(self.runid, topic, brokerHost,
                                           brokerPort)

    def tellDone(self, success):
        """
        alert the JobOffice that assigned job is done
        """
        self.jobSender.send(createJobDoneEvent(self.name, success))

class _GetAJobComp(object):

    def setup(self):
        deffile = DefaultPolicyFile("ctrl_sched","GetAJob_dict.paf","policies")
        defpol = Policy.createPolicy(deffile, deffile.getRepositoryPath())

        if not hasattr(self,"policy") or self.policy:
            self.policy = Policy()
        self.policy.mergeDefaults(defpol.getDictionary())

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

        topic = self.policy.getString("pipelineEvent")
        self.client = GetAJobClient(self.getRun(), self.getName(), topic,
                                    self.getEventBrokerHost())
        self.log.log(Log.INFO-1,
                     "Using OriginatorId = " % self.client.getOriginatorId())

    def setAssignment(self, clipboard):
        self.client.tellReady()
        jobid, inputs, outputs = self.client.getAssignment()
        clipboard.put("originatorId", self.client.getOriginatorId())
        clipboard.put(self.clipboardKeys["inputDatasets"], inputs)
        clipboard.put(self.clipboardKeys["outputDatasets"], outputs)
        clipboard.put(self.clipboardKeys["jobIdentity"], jobid)
        

class GetAJobParallelProcessing(harnessStage.ParallelProcessing, _GetAJobComp):
    """
    Stage implementation that gets a job assignment for processing by
    the parallel Slice.
    """
    def process(self, clipboard):
        """
        get the job assignment and post it to the clipboard.
        """
        self.setAssignment(clipboard)

class GetAJobSerialProcessing(harnessStage.SerialProcessing, _GetAJobComp):
    """
    Stage implementation that gets a job assignment for processing by
    the master Pipeline thread.
    """
    def preprocess(self, clipboard):
        """
        get the job assignment and post it to the clipboard.
        """
        self.setAssignment(clipboard)

class _DataReadyComp(object):

    def setup(self, policyDict="DataReady_dict.paf"):
        deffile = DefaultPolicyFile("ctrl_sched", policyDict, "policies")
        defpol = Policy.createPolicy(deffile, deffile.getRepositoryPath())

        if not hasattr(self,"policy") or self.policy:
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
            if len(possible) < 1:
                break
            possible = client.tellDataReady(possible, completed)

        # update the possible list for the ones we have not reported
        # on yet.
        clipboard.set(self.clipboardKeys["possibleDatasets"], possible)
       

class DataReadyParallelProcessing(harnessStage.ParallelProcessing, _DataReadyComp):
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

class DataReadySerialProcessing(harnessStage.SerialProcessing, _DataReadyComp):
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
        if clipboard and len(self.dataclients) > 0:
            self.tellDataReady(clipboard)
        self.jobclient.tellDone(self.jobSuccess)

class JobDoneParallelProcessing(harnessStage.ParallelProcessing, _JobDoneComp):
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

class JobDoneSerialProcessing(harnessStage.SerialProcessing, _JobDoneComp):
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

