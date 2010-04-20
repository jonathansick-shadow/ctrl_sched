#! /usr/bin/env python
#
from __future__ import with_statement
import sys, os, time, datetime
import optparse, traceback
import lsst.pex.harness.run as run
from lsst.pex.logging import Log, LogRec
from lsst.pex.exceptions import LsstException
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events

usage = """Usage: %prog [-vqsd] [-V int] [-w seconds] [-r runid] broker topic ..."""

desc = """listen for and print events and their properties."""

cl = optparse.OptionParser(usage=usage, description=desc)
run.addAllVerbosityOptions(cl, "V")
cl.add_option("-w", "--wait-time", action="store", type="int", default=10, 
              dest="sleep", metavar="seconds",
              help="seconds to sleep when no events available (def: 10)")
cl.add_option("-r", "--runid", action="store", default=None, 
              dest="runid", help="restrict events to those with this Run ID")

logger = Log(Log.getDefaultLog(), "showEvents")
VERB = logger.INFO-2
timeoffset = time.time()

def main():
    """execute the showEvents script"""

    try:
        (cl.opts, cl.args) = cl.parse_args()
        Log.getDefaultLog().setThreshold(
            run.verbosity2threshold(cl.opts.verbosity, 0))

        showEvents(cl.args[0], cl.args[1:], cl.opts.runid, cl.opts.sleep)

    except run.UsageError, e:
        print >> sys.stderr, "%s: %s" % (cl.get_prog_name(), e)
        sys.exit(1)
    except Exception, e:
        logger.log(Log.FATAL, str(e))
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)

def showEvents(broker, topics, runid=None, sleep=10):
    """
    listen for and print events and their properties
    @param broker   the host where the event broker is running
    @param topics   a list (or space-delimited string) of event topics to
                       listen for
    @parma sleep    seconds to sleep when no events are available
    """
    if not isinstance(topics, list):
        topics = topics.split()

    logger.log(VERB, "Watching for events: " + ", ".join(topics))

    eventRcvrs = makeReceivers(broker, topics, runid)
    listen(eventRcvrs, sleep)

def makeReceivers(broker, topics, runid=None):

    out = []
    select = None
    if runid:
        select = "RUNID = '%s'" % runid
    for topic in topics:
        if select:
            rcvr = events.EventReceiver(broker, topic, select)
        else:
            rcvr = events.EventReceiver(broker, topic)
        out.append(rcvr)
    return out

def listen(receivers, sleep):
    try:
        while True:
            if checkTopics(receivers) == 0:
                time.sleep(sleep)
    except KeyboardInterrupt:
        logger.log(VERB, "KeyboardInterrupt: stopping event monitoring")


def checkTopics(receivers):
    thresh = logger.getThreshold()
    quiet = thresh >= logger.WARN
    loud = thresh <= VERB
    silent = thresh > logger.FATAL
    count = 0

    for rcvr in receivers:
        logger.log(logger.DEBUG, "looking for " + rcvr.getTopicName())
        event = rcvr.receiveEvent(0)
        if event:
            ts = time.time()
            date = str(datetime.datetime.utcfromtimestamp(ts))
            ts -= timeoffset
            if event.getPropertySet().exists("TIMESTAMP"):
                ts = event.getPropertySet().get("TIMESTAMP") / 1.0e9
                date = str(datetime.datetime.utcfromtimestamp(ts))
                ts -= timeoffset
            if event.getPropertySet().exists("DATE"):
                date = event.getPropertySet().get("DATE")
            count += 1

            if silent:
                continue
            print "%s: DATE=%s, TIMESTAMP=%f" % (rcvr.getTopicName(), date, ts)
                                                 
            if not quiet:
                ps = event.getPropertySet()
                print ps.toString()

    return count

if __name__ == "__main__":
    main()
