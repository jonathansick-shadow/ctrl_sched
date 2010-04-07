"""
tools and stages for pipelines that interact with the JobOffice scheduler
"""
import lsst.pex.harness.stage as harnessStage
from dataset import Dataset
import lsst.ctrl.sched.utils as utils
from lsst.ctrl.events as EventSystem, EventReceiver
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
        and return the assigned datasets to process.
        """
        event = self.rcvr.receiveStatusEvent()
        if not event:
            return None

        return utils.unserializeDatasetList(
            event.getPropertySet().getArrayString("dataset"))

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

        self.dataSender = utils.EventSender(self.runid, topic, brokerHost,
                                            brokerPort)


    def tellDataReady(self, datasets, success=False):
        """
        alert JobOffices that one or more datasets are ready.
        @param datasets    a single or list of dataset objects
        @param success     True if all of the datasets were successfully
                              created.
        """
        return self.dataSender.send(
            self.sender.createDatasetEvent(self.name, datasets, success))


class JobDoneClient(DataReadyClient):
    """
    a component working on the behalf of a pipeline to alert JobOffices
    that a job is finished.  It can also signal what datasets were created.

    @see JobDoneStage
    """

    def __init__(self, runId, pipelineName, dataTopic, jobTopic, brokerHost,
                 brokerPort=None):
        """
        create the client
        @param runId         the Run ID for the pipeline
        @param pipelineName  the logical name of the pipeline
        @param topic         the topic to be used to communicate with
                                 the JobOffice
        """
        DataReadyClient.__init__(self, runId, pipelineName, dataTopic,
                                 brokerHost, brokerPort=brokerPort)
                                 
        self.jobSender = utils.EventSender(self.runid, jobTopic, brokerHost,
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

        self.clipboardKey = self.policy.getString("datasetsClipboardKey")

        topic = self.policy.getString("pipelineEvent")
        self.client = GetAJobClient(self.getRun(), self.getName(), topic,
                                    self.getEventBrokerHost())
        

    def setAssignment(self, clipboard):
        self.client.tellReady()
        datasets = self.client.getAssignment()
        clipboard.put("originatorId", self.client.getOriginatorId())
        clipboard.put(self.clipboardKey, datasets)
        

class GetAJobParallelProcessing(harnessStage.ParallelProcessing, _GetAJobComp)
    """
    Stage implementation that gets a job assignment for processing by
    the parallel Slice.
    """
    def process(self, clipboard):
        """
        get the job assignment and post it to the clipboard.
        """
        self.setAssignment(clipboard)

class GetAJobSerialProcessing(harnessStage.SerialProcessing, _GetAJobComp)
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

    def setup(self):
        deffile = DefaultPolicyFile("ctrl_sched","DataReady_dict.paf","policies")
        defpol = Policy.createPolicy(deffile, deffile.getRepositoryPath())

        if not hasattr(self,"policy") or self.policy:
            self.policy = Policy()
        self.policy.mergeDefaults(defpol.getDictionary())

#        self.mode = self.policy.getString("mode")
#        if self.mode not in "parallel serial":
#            raise RuntimeError("Stage %s: Unsupported mode: %s" %
#                               (self.getName(), self.mode))

        topic = self.policy.getString("dataReadyEvent")
        self.client = DataReadyClient(self.getRun(), self.getName(), topic,
                                      self.getEventBrokerHost())
        
#    def tellAllReady(self, success=None):
#        """
#        send even 
#        """
