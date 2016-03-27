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
stages for a test pipeline that demonstrates interactions with a JobOffice
scheduler.  
"""
import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
from lsst.ctrl.sched import Dataset

import os
import sys
import time


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
        if inputs:
            for ds in inputs:
                self.mylog.log(Log.INFO, "Loading " + ds.toString())
        else:
            self.mylog.log(Log.WARN, "No input datasets given")


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
        if self.visitCount == self.failOnVisitN:
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
        if expected:
            for ds in expected:
                self.mylog.log(Log.INFO, "Writing out " + ds.toString())
                outputds.append(ds)
        else:
            self.log.log(Log.WARN, "No expected datasets on clipboard")

        clipboard.put(self.outputDatasetsKey, outputds)


class FakeOutputStage(harnessStage.Stage):
    parallelClass = FakeOutput




