"""
utility functionality, include a class for sending events used by the
scheduling framework.  
"""
from __future__ import with_statement

import lsst.ctrl.events as ev
from lsst.daf.base import PropertySet
from lsst.ctrl.sched import Dataset
from lsst.pex.policy import Policy, PolicyString, PAFWriter

import os, time, random

def serializePolicy(policy):
    """
    write a Policy to a PAF-encoded string.  This is useful for encoding 
    Policy data into PropertySets.
    """
    writer = PAFWriter()
    writer.write(policy)
    return writer.toString()

def unserializePolicy(policystr):
    """
    turn PAF-serialized string back into a Policy.  This is the opposite
    of serializePolicy().
    """
    return Policy.createPolicy(PolicyString(policystr))

def serializeDataset(dataset):
    """
    write a Dataset to a PAF-encoded string.  This is useful for encoding 
    Dataset objects into PropertySets.
    """
    return serializePolicy(dataset.toPolicy())

def unserializeDataset(datasetstr):
    """
    turn PAF-serialized string back into a Dataset.  This is the opposite
    of serializeDataset().
    """
    return Dataset.fromPolicy(unserializePolicy(datasetstr))

def serializeDatasetList(datalist):
    """
    convert a list of Datasets into a list of PAF-encoded strings.  This is
    useful for encoding Dataset data into PropertySets.
    """
    return map(lambda d: serializeDataset(d), datalist)

def unserializeDatasetList(dstrlist):
    """
    convert a list of PAF-encoded strings into a list of Datasets.  This is
    the opposite of serializeDatasetList().
    """
    return map(lambda d: unserializeDataset(d), dstrlist)


def createRunId(base="test", lim=100000):
    """
    create unique run identifier
    @param base   use this as an identifier prefix
    """
    width = len(str(lim))-1
    fmt = "%%s%%0%dd" % width
    return fmt % (base, random.randrange(lim))
    

class EventSender(object):
    """
    the class makes it easy to send multiple events to stimulate or simulate
    the working JobOffice process.
    """

    def __init__(self, runid, topic, brokerhost, originatorId=None,
                 brokerport=0):
        """
        create a sender that will send events to a given broker on a given
        topic.
        @param runid       the Run ID to send in events
        @param topic       the event topic
        @param brokerhost  the hostname where the event broker is running
        @param originatorId  the number to use as the Originator ID
        @param brokerport  the port that the broker is listening on. 
        """
        self.runid = runid
        self.esys = ev.EventSystem.getDefaultEventSystem()
        if originatorId is None:
            originatorId = self.esys.createOriginatorId()
        self.origid = originatorId
        if brokerport and brokerport > 0:
            self.trxr = ev.EventTransmitter(brokerhost, brokerport, topic)
        else:
            self.trxr = ev.EventTransmitter(brokerhost, topic)

    def send(self, event):
        """
        send out the event.
        """
        if isinstance(event, _EventFactory):
            self.trxr.publishEvent(event.create())
        else:
            self.trxr.publishEvent(event)

    def createStatusEvent(self, status, props=None, originatorId=None):
        """
        create a candidate status event of a given status.

        This actually returns an event factory class.
        """
        if not originatorId:
            originatorId = self.origid
        return _StatusEventFactory(self.runid, status, originatorId, props)

    def createCommandEvent(self, status, destid, props=None,
                           originatorId=None):
        """
        create a candidate command event of a given status.

        This actually returns an event factory class.
        """
        if not originatorId:
            originatorId = self.origid
        return _CommandEventFactory(self.runid,status,originatorId,destid,props)

    def createStopEvent(self, pipelineName, destid=None, urgency=1):
        """
        create a Stop command event
        @param destid    the destination ID (optional)
        @param props     the properties to include along wiht the Event
        """
        props = {"pipelineName": pipelineName, "level": urgency }
        return _CommandEventFactory(self.runid,"stop",self.origid,destid,props)

    def createPipelineReadyEvent(self, pipelineName, originatorId=None):
        """
        create a candidate event for signalling that a pipeline is ready
        for a job.

        This actually returns an event factory class.
        """
        return self.createStatusEvent("job:ready",
                                      {"pipelineName": pipelineName},
                                      originatorId)

    def createJobAssignEvent(self, pipelineName, pipelineId, identity=None,
                             inputs=None, outputs=None, originatorId=None):
        """
        create a candidate event for assigning a job to a pipeline.

        This actually returns an event factory class.
        """
        out = self.createCommandEvent("job:assign", pipelineId, 
                                      {"pipelineName": pipelineName},
                                      originatorId)
        if identity:
            out.addDataset("identity", identity)
        if inputs:
            if not isinstance(inputs, list):
                inputs = [inputs]
            for ds in inputs:
                out.addDataset("inputs", ds)
        if inputs:
            if not isinstance(outputs, list):
                outputs = [outputs]
            for ds in outputs:
                out.addDataset("outputs", ds)

        return out

    def createJobAcceptEvent(self, pipelineName, originatorId=None):
        """
        create a candidate event for signalling that a pipeline is ready
        for a job.

        This actually returns an event factory class.
        """
        return self.createStatusEvent("job:accepted",
                                      {"pipelineName": pipelineName},
                                      originatorId)

    def createJobDoneEvent(self, pipelineName, success=True,originatorId=None):
        """
        create a candidate event for signalling that a pipeline is ready
        for a job.

        This actually returns an event factory class.
        """
        return self.createStatusEvent("job:done",
                                      {"pipelineName": pipelineName,
                                       "success": success            },
                                      originatorId)

    def createDatasetEvent(self, pipelineName, datasets=None, success=True,
                           originatorId=None):
        """
        create a candidate event for signalling that a pipeline is ready
        for a job.

        This actually returns an event factory class.
        """
        out = self.createStatusEvent("available",
                                     {"pipelineName": pipelineName,
                                      "success": success            },
                                     originatorId)

        if datasets:
            if not isinstance(datasets, list):
                datasets = [datasets]
            for ds in datasets:
                out.addDataset("dataset", ds)

        return out
    

class _EventFactory(object):

    def __init__(self, runid, props=None):
        """
        create a generic event factor
        """
        self.runid = runid

        if isinstance(props, PropertySet):
            self.props = props
        else:
            self.props = PropertySet()
            if isinstance(props, dict):
                for key in props.keys():
                    self.props.set(key, props[key])

    def create(self):
        """create a new instance of the event"""
        return ev.Event(self.runid, self.props)

    def setRunId(self, id):
        """set the Run ID"""
        self.runid = runid
    def getRunId(self):
        """set the Run ID"""
        return self.runid

    def setProperty(self, name, val):
        """set the value of a named property"""
        self.props.set(name, val)
    def getProperty(self, name):
        """get the value of a named property"""
        return self.props.getString(name)

    def addDataset(self, propname, ds):
        """add a dataset to the event"""
        self.props.add(propname, serializeDataset(ds))
    def getDatasets(self, propname):
        """return the datasets attached to the event"""
        return unserializeDatasetList(self.props.getArrayString(propname))

class _StatusEventFactory(_EventFactory):
    """
    create a factory for creating status events
    """

    def __init__(self, runid, status, originator, props=None):
        """create the factory"""
        _EventFactory.__init__(self, runid, props)
        self.props.set(ev.Event.STATUS, status)
        self.origid = originator

    def create(self):
        """create a new instance of the event"""
        return ev.StatusEvent(self.runid, self.origid, self.props)

    def getStatus(self):
        """return the value of the STATUS property"""
        return self.getProperty(ev.Event.STATUS)
    def setStatus(self, val):
        """set the value of the STATUS property"""
        return self.setProperty(ev.Event.STATUS, val)

    def getOriginatorId(self):
        """return the value of the originator ID"""
        return self.origid
    def setOriginatorId(self, val):
        """set the value of the originator ID"""
        self.origid = val

class _CommandEventFactory(_StatusEventFactory):
    """
    create a factory for creating status events
    """

    def __init__(self, runid, status, originator, destination, props=None):
        """create the factory"""
        _StatusEventFactory.__init__(self, runid, status, originator, props)
        self.destid = destination

    def create(self):
        """create a new instance of the event"""
        return ev.CommandEvent(self.runid, self.origid,self.destid, self.props)

    def getDestinationId(self):
        """return the value of the destination ID"""
        return self.destid
    def setDestinationId(self, val):
        """set the value of the destination ID"""
        self.destid = val

