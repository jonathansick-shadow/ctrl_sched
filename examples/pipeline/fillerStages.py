"""
stages for a test pipeline that demonstrates interactions with a JobOffice
scheduler.  
"""
import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
from lsst.ctrl.sched.dataset import Dataset


class FakeInput(harnessStage.ParallelProcessing):
    """
    this stage logs the dataset we're supposed to be reading in
    """

    def setup(self):
        if not self.log:
            self.log = Log.getDefaultLog()
        self.mylog = Log(self.log, "inputStage")
        self.inputDatasetKey = \
                    self.policy.getString("inputKeys.inputDatasets")

    def process(self, clipboard):
        inputs = clipboard.get(self.inputDatasetKey)
        for ds in inputs:
            self.mylog.log(Log.INFO, "Loading " + ds.toString())

class FakeInputStage(harnessStage.Stage):
    parallelClass = FakeInput

class FakeProcessing(harnessStage.ParallelProcessing):
    """
    this stage simulates work by sleeping
    """

    def setup(self):
        if not self.log:
            self.log = Log.getDefaultLog()
        self.mylog = Log(self.log, "fakeProcess")
        self.jobIdentityItem = \
                    self.policy.getString("inputKeys.jobIdentity")
        self.sleeptime = self.policy.getInt("sleep")
        self.visitCount = 0
        self.failOnVisitN = self.policy.getInt("failIteration")
        

    def process(self, clipboard):
        jobIdentity = clipboard.get(self.jobIdentityItem)
        self.mylog.log(Log.INFO, "Processing %s %s..." % (jobIdentity["type"], str(jobIdentity)))
        time.sleep(self.sleeptime)

        self.visitCount += 1
        if self.visitCount == self.failOnVisit+1:
            raise RuntimeError("testing failure stage")

class FakeProcessingStage(harnessStage.Stage):
    parallelClass = FakeProcessing

class FakeOutput(harnessStage.ParallelProcessing):
    """
    this stage simulates work by sleeping
    """

    def setup(self):
        if not self.log:
            self.log = Log.getDefaultLog()
        self.mylog = Log(self.log, "output")
        self.outputDatasetsKey = \
                    self.policy.getString("inputKeys.outputDatasets")
        self.possibleDatasetsKey = \
                    self.policy.getString("inputKeys.possibleDatasets")

    def process(self, clipboard):
        expected = clipboard.get(self.possibleDatasetsKey)
        outputds = clipboard.get(self.outputDatasetsKey)

        # this implementation will pretend to write out all of the
        # expected datasets.  It will also put each dataset written
        # out into the outputDatasets list.
        for ds in expected:
            self.mylog.log(Log.INFO, "Writing out " + ds.toString())
            outputds.append(ds)

        clipboard.set(self.outputDatasetsKey, outputds)

class FakeOutputStage(harnessStage.Stage):
    parallelClass = FakeOutput

    


