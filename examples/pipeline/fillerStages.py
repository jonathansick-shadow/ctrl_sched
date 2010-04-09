"""
stages for a test pipeline that demonstrates interactions with a JobOffice
scheduler.  
"""
import lsst.pex.harness.stage as harnessStage
from lsst.ctrl.sched.dataset import Dataset


class FakeInputStage(harnessStage.ParallelProcessing):
    """
    this stage logs the dataset we're supposed to be reading in
    """

    def setup(self):
        self.mylog = Log(self.log, "inputStage")
        self.inputDatasetKey = \
                    self.policy.getString("outputKeys.inputDatasets")

    def process(self, clipboard):
        inputs = clipboard.get(self.inputDatasetKey)
        for ds in inputs:
            self.mylog.log(Log.INFO, "Loading " + ds.toString())




class FakeProcessing(harnessStage.ParallelProcessing):
    """
    this stage simulates work by sleeping
    """

    def setup(self):
        self.mylog = Log(self.log, "fakeProcess")
        self.jobIdentityItem = \
                    self.policy.getString("inputKeys.jobIdentity")
        self.sleeptime = self.policy.getInteger("sleep")
        self.visitCount = 0
        self.failOnVisitN = self.policy.getInt("failIteration")
        

    def process(self, clipboard):
        jobIdentity = clipboard.get(self.jobIdentityItem)
        self.mylog.log(Log.INFO, "Processing %s %s..." % (jobIdentity["type"], str(jobIdentity)))
        time.sleep(self.sleeptime)

        self.visitCount += 1
        if self.visitCount == self.failOnVisit+1:
            raise RuntimeError("testing failure stage")

class FakeOutput(harnessStage.ParallelProcessing):
    """
    this stage simulates work by sleeping
    """

    def setup(self):
        self.mylog = Log(self.log, "output")
        self.outputDatasetsKey = \
                    self.policy.getString("inputKeys.outputDatasets")
        self.targetDatasetsKey = \
                    self.policy.getString("inputKeys.targetDatasets")

    def process(self, clipboard):
        expected = clipboard.get(self.targetDatasetsKey)
        outputds = clipboard.get(self.outputDatasetsKey)

        # this implementation will pretend to write out all of the
        # expected datasets.  It will also put each dataset written
        # out into the outputDatasets list.
        for ds in expected:
            self.mylog.log(Log.INFO, "Writing out " + ds.toString())
            outputds.append(ds)

        clipboard.set(self.outputDatasetsKey, outputds)

    


