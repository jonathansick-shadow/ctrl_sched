#!/usr/bin/env python
#
from __future__ import with_statement
import sys, os, time, re
import optparse, traceback

from lsst.pex.logging import Log, DualLog

from lsst.ctrl.sched.utils import EventSender
from lsst.ctrl.sched import Dataset

usage = """%prog [-vqsf] [-b brokerhost] [-p brokerport] [-i iddelim] [-e eqdelim] [-I secs] [-t topic] [-D dataset ...] -r runid listfile ..."""

desc = """Send dataset-available events.  Each given list file will be processed in order.  Any datasets provided via -D are alerted before those in the list files.  The options -i, -e, -I, -f and t apply to the datasets given via -D and all datasets in the list files where these attributes have not set within the files themselves.  A runid must always be specified via the -r option."""

cl = optparse.OptionParser(usage=usage, description=desc)
cl.add_option("-v", "--verbose", action="store_const", default=0, 
              const=Log.DEBUG, dest="verb", help="print extra status messages")
cl.add_option("-q", "--quiet", action="store_const", 
              const=Log.WARN, dest="verb",
              help="limit screen messages to error messages")
cl.add_option("-s", "--silent", action="store_const", 
              const=Log.FATAL+1, dest="verb",
              help="limit screen messages to error messages")
cl.add_option("-b", "--broker-host", action="store", dest="brokerhost", 
              help="hostname where event broker is running")
cl.add_option("-p", "--broker-port", action="store", type=int,
              dest="brokerport", 
              help="port number where event broker is listening")
cl.add_option("-r", "--runid", action="store", default=None, dest="runid",
              help="the runid the pipelines were launched under")
cl.add_option("-t", "--topic", action="store", metavar="TOPIC", default=None,
              dest="topic",
              help="the default topic to send events to.  This can be over-ridden by the dataset files.")
cl.add_option("-I", "--interval", metavar="SEC", action="store", default=0,
              type='float', dest="interval",
              help="the default time gap to insert between events.  This can be over-ridden by the dataset files.")
cl.add_option("-f", "--tell-fail", action="store_true", default=False, 
              dest="fail",
              help="if set, datasets will be marked as failed by default.  This can be over-ridden by the dataset files.")
cl.add_option("-i", "--id-delim", action="store", dest="iddelim", default=None,
              metavar="CHARS",
              help="the default delimiters to assume separate the dataset ids in the dataset lists")
cl.add_option("-e", "--eq-delim", action="store", dest="eqdelim", default="=",
              metavar="CHARS",
              help="the default delimiters to assume separate a dataset id name from its value")
cl.add_option("-F", "--format", action="store", dest="format",
              metavar="DATADESC", 
              help="the default dataset description format; see format directive via -H")
cl.add_option("-D", "--dataset", action="append", dest="datasets",
              metavar="DATADESC", 
              help="a dataset to send an event for, in lieu of or before datasets given in the dataset list files")
cl.add_option("-H", "--syntax-help", action="store_true", default=False, 
              dest="synhelp",
              help="print help on dataset list file syntax and then exit.  All other inputs are ignored")


logger = Log(Log.getDefaultLog(), "announceDataset")

def main():
    (cl.opts, cl.args) = cl.parse_args()

    if cl.opts.synhelp:
        syntaxHelp()
        sys.exit(0)

    if cl.opts.verb:
        logger.setThreshold(cl.opts.verb)

    if cl.opts.brokerhost is None:
        cl.opts.brokerhost = 'lsst8.ncsa.uiuc.edu'
        warn("No broker host specified, using '%s'", cl.opts.brokerhost)

    if cl.opts.runid is None:
        fail("No runid specified")

    # set the default control attributes
    ctrl = {}
    for name in "topic interval iddelim eqdelim".split():
        ctrl[name] = getattr(cl.opts, name)
    ctrl["success"] = not cl.opts.fail
    ctrl["runid"] = cl.opts.runid
    ctrl["sender"] = None
    ctrl["host"] = cl.opts.brokerhost
    ctrl["port"] = cl.opts.brokerport
    ctrl["name"] = cl.prog or "announceDataset"
    ctrl["format"] = makeFormat(cl.opts.format)
    ctrl["intids"] = []

    numberSent = 0

    # process the dataset given on the command line
    if cl.opts.datasets:
        numberSent += sendEventsFor(ctrl, cl.opts.datasets)

    # now process each file
    okay = True
    for filename in cl.args:
        try:
            file = open(filename)
            try:
                numberSent += sendEventsFor(ctrl, file)
            finally:
                file.close()
        except EnvironmentError, ex:
            okay = False
            warn("trouble opening %s: %s", (filename, str(ex.strerror)))
        except Exception, ex:
            okay = False
            traceback.print_exc()
            warn("trouble parsing %s: %s", (filename, str(ex)))

    return okay


def sendEventsFor(data, lines):
    count = 0
    ctrl = data.copy()
    sender = None

    for line in lines:
        line = line.strip()
        line = line.rsplit('#')[0]
        if len(line) == 0:
            continue
        if line[0] == '>':
            updateControlData(ctrl, line[1:].strip())

        else:
            if not ctrl["sender"]:
                debug("Sending events to the '%s' topic", ctrl["topic"])
                ctrl["sender"] = EventSender(ctrl["runid"], ctrl["topic"],
                                             ctrl["host"], ctrl["port"])
                sender = ctrl["sender"]

            dss = toDatasets(line, ctrl)
            if dss:
                if not ctrl["success"]:
                    for ds in dss:
                        ds.valid = False
                if ctrl["interval"]:
                    time.sleep(ctrl["interval"])

                ev = sender.createDatasetEvent(ctrl["name"], dss,
                                               ctrl["success"])
                inform("sending event for %s", dss[0])
                sender.send(ev)
                count += len(dss)
                
            else:
                debug("No dataset parsed from dataset line: %s", line)

    return count

directives = "topic pause success fail interval iddelim eqdelim intids format".split()

def updateControlData(ctrl, line):
    args = line.split(None, 1)
    if len(args) == 0:
        raise ValueError("Empty directive line")
    cmd = args.pop(0).lower()
    args = len(args) > 0 and args[0] or ''

    cmds = filter(lambda c: c.startswith(cmd), directives)
    if len(cmds) < 1:
        raise ValueError("unrecognized directive name: " + cmd)
    if len(cmds) > 1:
        raise ValueError("ambiguous directive name: %s; (which of %s is it?)" %
                         (cmd, cmds))
    cmd = cmds[0]

    if cmd in "interval iddelim eqdelim topic":
        if not args:
            ctrl[cmd] = getattr(cl.opts, cmd)
        ctrl[cmd] = args

        if cmd == "interval":
            ctrl[cmd] = float(ctrl[cmd])
        elif cmd == "topic":
            ctrl["sender"] = None

    elif cmd == "pause":
        args = args.split()
        if len(args) < 1:
            raise ValueError("Missing argument to pause directive")
        if len(args) > 1:
            warn("Ignoring extra arguments to pause directive: " +
                 " ".join(args[1:]))

        try:
            wait = float(args[0])
        except ValueError, ex:
            raise ValueError("Bad argument to pause directive: " + args[0])
        debug("pausing %fs...", wait)
        time.sleep(wait)

    elif cmd == "success":
        if not args:
            ctrl["success"] = True
        else:
            val = args.strip().lower()
            ctrl["success"] = not ("false".startswith(val) or val == "0")
    elif cmd == "fail":
        if len(args) == 0:
            ctrl["success"] = False
        else:
            val = args[0].strip().lower()
            ctrl["success"] = "false".startswith(val) or val == "0"
    elif cmd == "intids":
        ctrl[cmd] = args.split()
    elif cmd == "format":
        if not args:
            ctrl[cmd] = None
        else:
            ctrl[cmd] = makeFormat(args)
        

cnvspec = re.compile(r"%\(([^\)]+)\)([#0\- \+])?(\d+|\*)?(\.(\d+|\*))?([hlL]?)([diouxXeEfFgGcrs])")

class FormatRe(object):
    def __init__(self, fmtstr):
        self.fmtstr = fmtstr
        self.re = fmtstr
        self.ids = {}

    def search(self, line):
        m = re.search(self.re, line)
        if not m:
            raise ValueError("dataset does not match format (%s): %s" %
                             (self.fmtstr, line))
        return m

    def parse(self, line):
        m = self.search(line)
        data = m.groupdict()

        tp = "unknown"
        if data.has_key("type"):
            tp = data["type"]
        out = Dataset(tp)
        
        del data["type"]
        out.ids = data

        for key in out.ids.keys():
            if self.ids[key] == 'i':
                try:
                    out.ids[key] = int(out.ids[key])
                except ValueError, e:
                    warn("Value is not an integer: %s", out.ids[key])
            elif self.ids[key] == 'f':
                try:
                    out.ids[key] = float(out.ids[key])
                except ValueError, e:
                    warn("Value is not a floating-point number: %s", out.ids[key])

        return out

    def addID(self, name, valtype):
        self.ids[name] = valtype
        
    

def makeFormat(fmtstr):
    if not fmtstr:
        return None
    fmt = FormatRe(fmtstr)

    m = cnvspec.search(fmt.re)
    while m:
        (idname, cnvflag, min, prec, max, lenmod, cnvtype) = m.groups()
        valtype = 's'

        if "srouxXcidfFgGeE".find(cnvtype) < 0:
            raise ValueError("conversion type, %s, not supported/recognized" %
                             cnvtype)

        if "srouxXc".find(cnvtype) >= 0:
            width = "+"
            if min is not None or max is not None:
                if min is None:  min = ''
                if max is None:  max = ''
                width = "{min,max}"
            expr = ".%s" % width

        elif cnvtype == "d" or cnvtype == "i":
            # integer format
            valtype = 'i'
            expr = ""
            if cnvflag is not None:
                if cnvflag.find('-') < 0 and cnvflag.find('0') < 0:
                    expr += " *"
                if cnvflag.find('+') >= 0:
                    expr += r"[+\-]"
                elif cnvflag.find(' ') >= 0:
                    if cnvflag.find('-') < 0 and cnvflag.find('0') < 0:
                        expr += "-?"
                    else:
                        expr += r"[ \-]"
                if cnvflag.find('-') < 0 and cnvflag.find('0') >= 0:
                    expr += "0*"
            else:
                expr += " *-?"
            expr += r"\d+"

        elif "fFgG".find(cnvtype) >= 0:
            # floating point format
            valtype = 'f'
            if max is None: max = "6"
            if max == '*':  max = None
            expr = ""
            if cnvflag.find('+') >= 0:
                expr += r"[+\-]"
            elif cnvflag.find(' ') >= 0:
                expr += r"[ \-]"
            if cnvflag.find('-') < 0 and cnvflag.find('0') >= 0:
                expr += "0*"
            expr += r"\d*\."
            if cnvflag.find('#') < 0:
                expr += "?"
            if max is not None:
                expr += "\d{%s}" % int(max)

        if "eEgG".find(cnvtype) >= 0:
            expon = r"[eE][+\-]\d\d"
            if cnvtype.lower() == 'g':
                expr += "(%s)?" % expon
            else:
                expr += expon

        fmt.addID(idname, valtype)

        expr = "(?P<%s>%s)" % (idname, expr)

        # substitute in the regular expression we just built
        fmt.re = cnvspec.sub(expr, fmt.re, 1)
        m = cnvspec.search(fmt.re)

    return fmt


def toDatasets(lines, ctrl, intids=None):
    if not isinstance(lines, list):
        lines = [lines]
    if intids is None:
        intids = ctrl["intids"]
    
    out = []
    if ctrl["format"]:
        for line in lines:
            dataset = ctrl["format"].parse(line)
            out.append(dataset)

    else:

        for line in lines:
            if ctrl["iddelim"]:
                args = line.split(ctrl["iddelim"])
            else:
                args = line.split()
            type = filter(lambda a: a.find(ctrl["eqdelim"]) < 0, args)
            if len(type) > 1:
                raise ValueError("Multiple dataset types given: " +
                                 " ".join(type))

            if len(type) == 0:
                type = ["unknown"]
            ds = Dataset(type[0])
            ds.ids = {}
            for arg in args:
                if arg.find(ctrl["eqdelim"]) < 0:
                    continue

                parts = arg.split(ctrl["eqdelim"], 1)
                ds.ids[parts[0]] = parts[1]

            if intids:
                # convert values of selected to integers
                for idname in intids:
                    if ds.ids.has_key(idname):
                        try:
                            ds.ids[idname] = int(ds.ids[idname])
                        except ValueError, ex:
                            raise ValueError("ID %s value is not an int: %s" %
                                             (idname, ds.ids[idname]))

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

def syntaxHelp(prog="announceDataset"):
    if prog:
        sys.stdout.write("%s: " % prog)
    print """Syntax for dataset list files:

A dataset file at its simplest is a list of datasets, one per line,
that this script will send an event for.  This file can also contain
special directives that control how the events are sent.  In general,
each listed dataset is a description of a dataset, which includes its
dataset type and a set of unique identifiers, in some format.  Note
that dataset can be given as a filepath; however, one must specify how
to extract the type and identifiers from the path.  The default format
for describing datasets is:

   <dataset-type> [<idname>=<idvalue> [...]]

That is, it is a dataset type name followed by zero or more name-value
pairs giving the dataset identifier name and its value, separated by
an equals sign.  For example:

   PostISR visit=8193 ccd=22 snap=0 amp=3

The characters that separate the "words" in a dataset listing can be
overridden by the id-delim directive, and the characters that separate
the names from their values can be overridden by the eq-delim
directive (see below).

Lines that start with a # symbol (after zero or more spaces) are
interpreted as comments whose contents are ignored.  Comments may also
appear at the end of dataset lines or directive lines, delimited by
the # sign.  That is, a pound sign in any line and everything after it
is removed before being interpreted.  Blank lines are ignored.  

A directive is any line that begins with a > symbol (after zero or
more spaces) and is followed by a directive name.  Depending on the
directive, this may require additional space-delimited arguments.  An
example directive line looks like this:

  >pause 5

In general, a directive applies to all datasets listed after the
directive, until it is reset.

The following directives are supported:

  topic <string>        The name of the event topic that all subsequent
                        dataset lines should be sent to.

  success <true|false>  All successive datasets will marked as valid
                        until the next success or fail directive.  The "true"
                        or "false" argument is optional; the default is
                        "true".  

  fail <true|false>     All successive datasets will marked as invalid
                        until the next success or fail directive.  The "true"
                        or "false" argument is optional; the default is
                        "true".  

  pause <number>        Pause for the given number of seconds before
                        acting on the next line.

  interval <number>     Set the normal pause interval, in seconds, to
                        wait between datasets.

  iddelim <string>      the characters that delimit the id assignments
                        in the default format.  The format directive
                        over-rides this.

  eqdelim <string>      the characters that segregate
                        The format directive over-rides this.

  intids <string> ...   the names of dataset IDs that should have integer
                        values.  

  format <string>       a python formatting string that indicates how to
                        extract the dataset identifier data from the
                        dataset lines.  Formatting structures that do not
                        include a mapping key will be ignored; the
                        mapping key gives the name of the identifier
                        that is encoded at that location.  If no format
                        string is provided, the format reverts to the default
                        format.
"""


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(2)

    
