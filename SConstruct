# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re, sys
import lsst.SConsUtils as scons

env = scons.makeEnv("ctrl_sched",
                    r"$HeadURL$",
                    [["pex_logging"],
                     ["pex_policy"]
                    ])

pkg = env["eups_product"]
for d in Split("tests"):
    if os.path.isdir(d):
        try:
            SConscript(os.path.join(d, "SConscript"))
        except Exception, e:
            print >> sys.stderr, "%s: %s" % (os.path.join(d, "SConscript"), e)

env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", [env.Install(env['prefix'], "python"),
                  env.Install(env['prefix'], "bin"),
                  env.InstallEups(env['prefix'] + "/ups")])



scons.CleanTree(r"*~ core *.so *.os *.o")

#
# Build TAGS files
#
files = scons.filesToTag()
if files:
    env.Command("TAGS", files, "etags -o $TARGET $SOURCES")

env.Declare()
