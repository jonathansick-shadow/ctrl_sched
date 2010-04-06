#!/usr/bin/env python
#
from __future__ import with_statement
import sys, os, time
import optparse, traceback

from lsst.pex.logging import Log, DualLog
from lsst.pex.policy import Policy, DefaultPolicyFile
import lsst.pex.harness.run as run
from lsst.ctrl.sched import *

usage = """usage %prog [-vqsD] [-L lev] [-l logfile] [-d dir] [-b brokerhost] [-p brokerport] -r runid policy_file"""

desc = """Run a Job Office for a pipeline according to a policy file"""

cl = optparse.OptionParser(usage=usage, description=desc)
cl.add_option("-v", "--verbose", action="store_true", default=False, 
              dest="toscreen", help="print all logging messages to screen")
cl.add_option("-q", "--quiet", action="store_const", default=0,
              const=Log.WARN, dest="screenverb",
              help="limit screen messages to error messages")
cl.add_option("-s", "--silent", action="store_const", 
              const=Log.FATAL+1, dest="screenverb",
              help="limit screen messages to error messages")
cl.add_option("-D", "--as-daemon", action="store_true", default=False, 
              dest="asdaemon",
              help="run as a daemon: fork a process and then exit the parent")
run.addVerbosityOption(cl, dest="logverb")
cl.add_option("-l", "--logfile", action="store", dest="logfile",
              help="use the given path as location of logfile")
cl.add_option("-d", "--datadir", action="store", dest="rootdir",
              help="root working directory for job offices")
cl.add_option("-b", "--broker-host", action="store", dest="brokerhost", 
              help="hostname where event broker is running")
cl.add_option("-p", "--broker-port", action="store", type=int,
              dest="brokerport", 
              help="port number where event broker is listening")
cl.add_option("-r", "--runid", action="store", default="unkn_run", dest="runid",
              help="the runid the pipelines were launched under")

logger = Log(Log.getDefaultLog(), "joboffice")

def main():
    """
    run the job office
    """
    (cl.opts, cl.args) = cl.parse_args()

    if len(cl.args) == 0:
        fail("Missing policy file")
    if not os.path.exists(cl.args[0]):
        fail("%s: policy file not found" % cl.args[0])
    if not os.path.exists(cl.opts.rootdir):
        fail("%s: root directory not found" % cl.opts.rootdir)
    if not cl.opts.runid:
        logger.log(Log.WARN, "No RunID given (via -r)")

    defpolf = DefaultPolicyFile("ctrl_sched", "JobOffice_dict.paf", "policies")
    policy = Policy.createPolicy(cl.args[0])
    policy.mergeDefaults(Policy.createPolicy(defpolf,
                                             defpolf.getRepositoryPath()))
    name = policy.getString("name")

    # create the logger(s)
    logfile = cl.opts.logfile
    if not logfile:
        logfile = os.path.join(cl.opts.rootdir, name, "joboffice.log")
    if not os.path.exists(logfile):
        if not os.path.exists(os.path.dirname(logfile)):
            os.makedirs(os.path.dirname(logfile))
    
    screenvol = cl.opts.toscreen and Log.DEBUG or Log.FATAL+1
    ofclogger = DualLog(logfile, Log.DEBUG, screenvol)
    ofclogger.setThreshold(run.verbosity2threshold(cl.opts.logverb, 0))

    try:
        # create the JobOffice instance
        office = createJobOffice(cl.opts.rootdir, policy, ofclogger, 
                                 cl.opts.runid, cl.opts.brokerhost,
                                 cl.opts.brokerport)
    except Exception, ex:
        logger.log(Log.FATAL, "Failed to create job office: " + str(ex))
        raise
        sys.exit(1)

    logger.log(Log.INFO, "Starting the Job Office...")
    try:
        # start up the office
        if cl.opts.asdaemon:
            pid = 0
            if cl.opts.toscreen:
                pid = os.fork()
            else:
                pid, fd = os.forkpty()

            if not pid:
                # in child
                office.run()
                if self.exc:
                    logger.log(Log.FATAL, str(office.exc))
                    sys.exit(1)
            else:
                logger.log(Log.DEBUG, "daemon launched.")
        else:
            office.start()
            try:
                time.sleep(1.0)
                if not office.isAlive():
                    if office.exc:
                        logger.log(Log.FATAL, str(office.exc))
                        sys.exit(1)
                    else:
                        logger.log(Log.WARN, "Exiting sooner than expected")
                while True:
                    time.sleep(15)
                    if not office.isAlive():
                        if office.exc:
                            logger.log(Log.FATAL, str(office.exc))
                            sys.exit(1)
                        else:
                            logger.log(Log.INFO, "Exiting normally")
                            break

            except Exception, ex:
                logger.log(Log.FATAL, str(ex))
                if office.isAlive():
                    office.stop()
                    office.join(30)
                sys.exit(1)
            except KeyboardInterrupt, ex:
                logger.log(Log.WARN, "Keyboard Interrupt: shutting down nicely...")
                if office.isAlive():
                    office.stop()
                    office.join(60)
                sys.exit(0)


    except Exception, ex:
        logger.log(Log.FATAL, str(ex))
        if office.isAlive():
            office.stop()
            office.join(30)
        sys.exit(1)

def fail(msg):
    logger.log(Log.FATAL, msg)
    sys.exit(1)

def createJobOffice(rootdir, policy, log, runId, brokerhost, brokerport):
    className = "DataTriggered"
    if policy.exists("className"):
        className = policy.getString("className")

    if not JobOffice.classLookup.has_key(className):
        fail("Unimplemented: unable to instantiate arbitrary class: " +
             className)

    cls = JobOffice.classLookup[className]
    return cls(rootdir, policy, log, runId, brokerhost, brokerport)


if __name__ == "__main__":
    main()
