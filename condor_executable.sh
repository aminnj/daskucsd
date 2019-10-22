#!/usr/bin/env bash

function getjobad {
    grep -i "^$1" "$_CONDOR_JOB_AD" | cut -d= -f2- | xargs echo
}

if [ -r "$OSGVO_CMSSW_Path"/cmsset_default.sh ]; then
    echo "sourcing environment: source $OSGVO_CMSSW_Path/cmsset_default.sh"
    source "$OSGVO_CMSSW_Path"/cmsset_default.sh
elif [ -r "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]; then
    echo "sourcing environment: source $OSG_APP/cmssoft/cms/cmsset_default.sh"
    source "$OSG_APP"/cmssoft/cms/cmsset_default.sh
elif [ -r /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
    echo "sourcing environment: source /cvmfs/cms.cern.ch/cmsset_default.sh"
    source /cvmfs/cms.cern.ch/cmsset_default.sh
else
    echo "ERROR! Couldn't find $OSGVO_CMSSW_Path/cmsset_default.sh or /cvmfs/cms.cern.ch/cmsset_default.sh or $OSG_APP/cmssoft/cms/cmsset_default.sh"
    exit 1
fi

if ! ls /hadoop/cms/store/ ; then
    echo "ERROR! hadoop is not visible, so the worker would be useless later. dying."
    exit 1
fi

ls -lrth
hostname

mkdir temp
cd temp

export SCRAM_ARCH=slc6_amd64_gcc700
export CMSSWVERSION=CMSSW_10_5_0
eval `scramv1 project CMSSW $CMSSWVERSION`
cd $CMSSWVERSION
eval `scramv1 runtime -sh`
cd -

mv ../workerenv.tar.* .
mv ../*.py .
echo "started extracting at $(date +%s)"
tar xf workerenv.tar.*
echo "finished extracting at $(date +%s)"

source workerenv/bin/activate

# # If tarball gets too big, could just xrdcp it...
# xrdcp root://redirector.t2.ucsd.edu//store/user/namin/RunIIAutumn18MiniAOD_ex9_vh_99.root .

ls -lrth
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/workerenv/bin:$PATH

echo $PATH
echo $PYTHONPATH

which python
which python3
which pip
which pip3
python -V
python3 -V

SCHEDULERADDRESS="$1"
dask-worker $SCHEDULERADDRESS --memory-limit 5GB --nprocs 1 --nthreads 1 --preload cachepreload.py

