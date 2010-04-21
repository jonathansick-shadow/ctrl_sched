#! /bin/sh
set -e
if [ -e ccdassembly-joboffice ]; then
   rm -rf ccdassembly-joboffice
fi

joboffice.py -D -L verb2 -r runid6 -b lsst8 -d . joboffice.paf
sleep 2
launchPipeline.py -L debug testPipeline.paf runid6 | grep -v Shutdown &

set +e
announceDataset.py -r runid6 -b lsst8.ncsa.uiuc.edu -t PostISRAvailable triggerdatasets.txt
sleep 15

# ps -auxww | grep runPipeline.py | grep runid6
pid=`ps -auxww | grep runPipeline.py | grep runid6 | awk '{print $2}'`
echo kill $pid
kill $pid
sendevent.py -b lsst8.ncsa.uiuc.edu -r runid6 stop JobOfficeStop
sleep 5
rm -rf ccdassembly-joboffice
