#!/usr/bin/env python
#
from __future__ import with_statement
import sys, os, time, re
import optparse, traceback

from lsst.pex.logging import Log, DualLog

from lsst.ctrl.sched.utils import EventSender
from lsst.ctrl.sched import Dataset

cmdnames = "ready|assign|dataset|done|stop"
usage = """usage %%prog [-vqsf] [-b brokerhost] [-p brokerport] [-r runid] [-i iddelim] %s topic [dataset ...]""" % cmdnames

desc = """Send a specified JobOffice-related event"""

cl = optparse.OptionParser(usage=usage, description=desc)
cl.add_option("-v", "--verbose", action="store_true", default=False, 
              dest="toscreen", help="print all logging messages to screen")
cl.add_option("-q", "--quiet", action="store_const", default=0,
              const=Log.WARN, dest="screenverb",
              help="limit screen messages to error messages")
cl.add_option("-s", "--silent", action="store_const", 
              const=Log.FATAL+1, dest="screenverb",
              help="limit screen messages to error messages")
cl.add_option("-n", "--pipeline-name", action="store", dest="name", 
              help="name of the pipeline one is communicating with")
cl.add_option("-b", "--broker-host", action="store", dest="brokerhost", 
              help="hostname where event broker is running")
cl.add_option("-p", "--broker-port", action="store", type=int,
              dest="brokerport", 
              help="port number where event broker is listening")
cl.add_option("-r", "--runid", action="store", default=None, dest="runid",
              help="the runid the pipelines were launched under")
cl.add_option("-f", "--tell-fail", action="store_true", default=False, 
              dest="fail",
              help="when applicable, set the success flag to False")
cl.add_option("-i", "--id-delim", action="store", dest="iddelim", default=" ",
              help="for dataset events, the delimiters look for to separate the datasets")
cl.add_option("-j", "--job-identity", action="store", dest="identity", 
              help="the identifiers and values that define the job being processed, in dataset format (for assign command)")
cl.add_option("-O", "--output-datasets", action="append", dest="outputs", 
              help="an output dataset, in dataset format; may appear multiple times (for assign command)")
cl.add_option("-o", "--orig-id", action="store", type="long", default=0L,
              dest="origid",
              help="for assign, the originator id to send to")

logger = Log(Log.getDefaultLog(), "sendevent")

def main():
    (cl.opts, cl.args) = cl.parse_args()

    if len(cl.args) < 1:
        fail("missing event type to send (ready|assign|dataset|done)")
    if len(cl.args) < 2:
        fail("missing event topic to send on")

    if not cl.opts.name:
        cl.opts.name = 'pipeline'
        warn("No pipeline name given; using '%s'", cl.opts.name)

    if cl.opts.runid is None:
        cl.opts.runid = "unkn_run"
        warn("No runid specified, using '%s'", cl.opts.runid)

    if cl.opts.brokerhost is None:
        cl.opts.brokerhost = 'lsst8.ncsa.uiuc.edu'
        warn("No broker host specified, using '%s'", cl.opts.brokerhost)

    cmd = filter(lambda c: c.startswith(cl.args[0]), cmdnames.split('|'))
    if len(cmd) == 0:
        fail("Unrecognized event type: %s", cl.args[0])
    elif len(cmd) > 1:
        fail("Ambiguously shortened event type given: %s", cl.args[0])
    cmd = cmd[0]

    sender = EventSender(cl.opts.runid, cl.args[1], cl.opts.brokerhost,
                         cl.opts.brokerport)

    ev = None
    if cmd == "ready":
        ev = sender.createPipelineReadyEvent(cl.opts.name)
    elif cmd == "assign":
        inputs = toDatasets(cl.args[2:])
        outputs = identity = None
        if cl.opts.outputs is not None:
            outputs = toDatasets(cl.opts.outputs)
        elif len(inputs) > 0:
            outputs = inputs[-1]
        if cl.opts.identity:
            identity = toDatasets([cl.opts.identity])
        elif len(inputs) > 0:
            identity = inputs[0]

        ev = sender.createJobAssignEvent(cl.opts.name, cl.opts.origid,
                                         identity, inputs, outputs)
    elif cmd == "dataset":
        ev = sender.createDatasetEvent(cl.opts.name, toDatasets(cl.args[2:]),
                                       not cl.opts.fail)
    elif cmd == "done":
        ev = sender.createJobDoneEvent(cl.opts.name, not cl.opts.fail)
    elif cmd == "stop":
        ev = sender.createStopEvent(cl.opts.name, cl.opts.origid)
    else:
        fail("no command!")

    inform("sending %s event", cmd)
    sender.send(ev)
    sys.exit(0)

def toDatasets(dsstrs, delim=r'\s', eqdelim='='):
    out = []
    delim = re.compile(r'[%s]' % delim)
    for dsstr in dsstrs:
        args = delim.split(dsstr)
        type = filter(lambda a: a.find(eqdelim) < 0, args)
        if len(type) > 1:
            fail("Dataset with multiple types specified: %s", dsstr)
        if len(type) == 0:
            type = ["unknown"]
        ds = Dataset(type[0])
        ds.ids = {}
        for arg in args:
            if arg.find(eqdelim) < 0:
                continue

            parts = arg.split(eqdelim, 1)
            try:
                parts[1] = int(parts[1])
            except ValueError:
                pass
            ds.ids[parts[0]] = parts[1]
        out.append(ds)

    return out

def _log(vol, msg, data=None):
    if data:
        logger.log(vol, msg % data)
    else:
        logger.log(vol, msg)

def fail(msg, data=None):
    _log(Log.FATAL, msg, data)
    sys.exit(1)

def warn(msg, data=None):
    _log(Log.WARN, msg, data)

def inform(msg, data):
    _log(Log.INFO, msg, data)

def debug(msg, data):
    _log(Log.DEBUG, msg, data)



if __name__ == "__main__":
    main()
