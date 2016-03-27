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
the actual Blackboard class
"""
from __future__ import with_statement

from queue import DataQueue, JobQueue
from item import Props
from exceptions import *
from lsst.pex.logging import Log
from lsst.utils.multithreading import LockProtected, SharedData

import os


class Blackboard(LockProtected):
    """
    a central persistable place to track jobs (and their immediate data
    dependencies) that need to be sent to a running pipeline.
    """

    def __init__(self, persistDir, logger=None, lock=SharedData()):
        """
        create an empty blackboard
        @param persistDir  a path to a directory where the blackboard's
                             state can persisted.
        @param logger      the logger to gather error messages
        @param lock        a SharedData instance to be used to protect this
                             instance from simultaneous updates by different
                             threads.  If not provided, a default is used.
        """
        LockProtected.__init__(self, lock)

        # the encompassing persistence directory
        self._persistDir = persistDir

        parent = os.path.dirname(self._persistDir)
        if not os.path.isdir(parent) or not os.path.exists(parent):
            raise BlackboardPersistError("Unable to create queue directory: %s: directory not found" % parent)
        if not os.path.exists(self._persistDir):
            os.mkdir(self._persistDir)
        elif not os.path.isdir(self._persistDir):
            raise BlackboardAccessError("Queue directory: %s: not a directory"
                                        % self._persistDir)

        # the logger to use
        if not logger:
            logger = Log.getDefaultLog()
        self._log = Log(logger, "blackboard")

        # prep the queues
        self.queues = lock
        with self.queues:

            # a queue representing available datasets to be processed.
            dir = os.path.join(self._persistDir, "dataAvailable")
            self.queues.dataAvailable = DataQueue(dir, self._log, lock)

            # a queue of datasets that have beend bundled into jobs and queued
            # for processing.
            dir = os.path.join(self._persistDir, "jobsPossible")
            self.queues.jobsPossible = JobQueue(dir, self._log, lock)

            # a queue of jobs that are ready to be processed
            dir = os.path.join(self._persistDir, "jobsAvailable")
            self.queues.jobsAvailable = JobQueue(dir, self._log, lock)

            # a queue of jobs that have been taken up by a pipeline and are
            # currently being processed.
            dir = os.path.join(self._persistDir, "jobsInProgress")
            self.queues.jobsInProgress = JobQueue(dir, self._log, lock)

            # a queue for jobs that have been completed by a pipeline
            dir = os.path.join(self._persistDir, "jobsDone")
            self.queues.jobsDone = JobQueue(dir, self._log, lock)

            # a queue of pipelines that are ready to accept a job
            dir = os.path.join(self._persistDir, "pipelinesReady")
            self.queues.pipelinesReady = JobQueue(dir, self._log, lock)

        self._dbfail = 0

    def makeJobAvailable(self, job):
        """
        transfer a job form the jobsPossible queue to the jobsAvailable
        queue.  If the job is not in the jobsPossible queue, an exception
        will be raised.  The moved job will be returned.
        """
        with self:
            with self.queues.jobsPossible:
                with self.queues.jobsAvailable:
                    try:
                        index = self.queues.jobsPossible.index(job)
                    except ValueError, ex:
                        raise BlackboardUpdateError("Job not found in jobsPossible: " +
                                                    job.getProperty(Props.NAME, "(unidentified)"))
                    job = self.queues.jobsPossible.pop(index)
                    self.queues.jobsAvailable.append(job)

    def allocateNextJob(self, pipelineId):
        """
        move the job at the front of the jobsAvailable queue to the
        jobsInProgress queue.
        @param pipelineId  the originator ID for the pipeline that is taking
                             this job.
        @return JobItem    the job that was moved 
        """
        with self:
            with self.queues.jobsAvailable:
                if self.queues.jobsAvailable.isEmpty():
                    raise EmptyQueueError("jobsAvailable")
                with self.queues.jobsInProgress:
                    job = self.queues.jobsAvailable.pop()
                    job.setPipelineId(pipelineId)
                    self.queues.jobsInProgress.append(job)
                    return job

    def rescheduleJob(self, job):
        """
        reshedule the given Job as done by moving it from the jobsInProgress
        queue to the jobsAvailable queue.  If the job is not in the 
        jobsInProgress queue, an exception will be raised.
        @param job      the job item to move
        @return JobItem    the job that was moved 
        """
        with self:
            with self.queues.jobsInProgress:
                with self.queues.jobsDone:
                    try:
                        index = self.queues.jobsInProgress.index(job)
                    except ValueError, ex:
                        raise BlackboardUpdateError("Job not found in jobsInProgress: " +
                                                    job.getProperty(Props.NAME, "(unidentified)"))
                    job = self.queues.jobsInProgress.pop(index)
                    self.queues.jobsAvailable.append(job)

    def markJobDone(self, job, success=True):
        """
        mark the given Job as done by moving it from the jobsInProgress
        queue to the jobsDone queue.  If the job is not in the jobsInProgress
        queue, an exception will be raised.
        @param job      the job item to move
        @param success  a flag indicating whether the job was completed
                          successfully.  If false, the job will be marked
                          as having failed.  
        @return JobItem    the job that was moved 
        """
        with self:
            with self.queues.jobsInProgress:
                with self.queues.jobsDone:
                    try:
                        index = self.queues.jobsInProgress.index(job)
                    except ValueError, ex:
                        raise BlackboardUpdateError("Job not found in jobsInProgress: " +
                                                    job.getProperty(Props.NAME, "(unidentified)"))
                    job = self.queues.jobsInProgress.pop(index)
                    job.markSuccessful(success)
                    self.queues.jobsDone.append(job)
