#! /usr/bin/env python

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
This tests the pipeline described in testPipeline.paf
"""
import os
import sys
import lsst.pex.harness as pexHarness
import lsst.pex.harness.stage as harnessStage
from lsst.pex.harness.simpleStageTester import SimpleStageTester
import lsst.pex.policy as pexPolicy
from lsst.pex.logging import Log, Debug, LogRec, Prop
from lsst.pex.exceptions import LsstCppException

from lsst.ctrl.sched.pipeline import GetAJobFromSliceStage, JobDoneFromSliceStage
from fillerStages import *


def main():

    # First create a tester.  For convenience, we use our special AreaStage
    # factory class (which is defined below) to configure the tester.
    #
    tester = SimpleStageTester(name="testPipeline", runID=sys.argv[1])
    tester.setEventBroker("lsst8.ncsa.uiuc.edu")

    stagePolicy = pexPolicy.Policy.createPolicy("getajob.paf")
    tester.addStage(GetAJobFromSliceStage(stagePolicy))

    stagePolicy = pexPolicy.Policy.createPolicy("filler.paf")
    tester.addStage(FakeInputStage(stagePolicy))
    tester.addStage(FakeProcessingStage(stagePolicy))
    tester.addStage(FakeOutputStage(stagePolicy))

    stagePolicy = pexPolicy.Policy.createPolicy("jobdone.paf")
    tester.addStage(JobDoneFromSliceStage(stagePolicy))

    # set the verbosity of the logger.  If the level is at least 5 you
    # will see debugging messages from the SimpleStageTester wrapper.
    tester.setDebugVerbosity(10)

    clipboard = {}
    clipboard = tester.runWorker(clipboard)


if __name__ == "__main__":
    main()
