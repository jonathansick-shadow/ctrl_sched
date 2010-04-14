#! /usr/bin/env python
"""
This tests the pipeline described in testPipeline.paf
"""
import os, sys
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
    tester = SimpleStageTester(runID=sys.argv[1])
    tester.setEventBroker("lsst8.ncsa.uiuc.edu")

    stagePolicy = pexPolicy.Policy.createPolicy("getajob.paf")
    tester.addStage( GetAJobFromSliceStage(stagePolicy) )

    stagePolicy = pexPolicy.Policy.createPolicy("filler.paf")
    tester.addStage( FakeInputStage(stagePolicy) )
    tester.addStage( FakeProcessingStage(stagePolicy) )
    tester.addStage( FakeOutputStage(stagePolicy) )
    
    stagePolicy = pexPolicy.Policy.createPolicy("jobdone.paf")
    tester.addStage( JobDoneFromSliceStage(stagePolicy) )
    
    # set the verbosity of the logger.  If the level is at least 5, you
    # will see debugging messages from the SimpleStageTester wrapper.
    tester.setDebugVerbosity(5)

    clipboard = { }
    clipboard = tester.runWorker(clipboard)





if __name__ == "__main__":
    main()
