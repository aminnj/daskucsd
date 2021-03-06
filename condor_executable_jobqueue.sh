#!/usr/bin/env bash

function getjobad {
    grep -i "^$1" "$_CONDOR_JOB_AD" | cut -d= -f2- | xargs echo
}

if [ -r "$OSGVO_CMSSW_Path"/cmsset_default.sh ]; then source "$OSGVO_CMSSW_Path"/cmsset_default.sh
elif [ -r "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]; then source "$OSG_APP"/cmssoft/cms/cmsset_default.sh
elif [ -r /cvmfs/cms.cern.ch/cmsset_default.sh ]; then source /cvmfs/cms.cern.ch/cmsset_default.sh
else
    echo "ERROR! Couldn't find $OSGVO_CMSSW_Path/cmsset_default.sh or /cvmfs/cms.cern.ch/cmsset_default.sh or $OSG_APP/cmssoft/cms/cmsset_default.sh"
    exit 1
fi

hostname

if ! ls /hadoop/cms/store/ ; then
    echo "ERROR! hadoop is not visible, so the worker would be useless later. dying."
    exit 1
fi

mkdir temp ; cd temp

tarballpath=$(getjobad tarballpath)

# mv ../{daskworkerenv.tar.*,*.py} .
# xrdcp root://redirector.t2.ucsd.edu//store/user/namin/dask/daskworkerenv.tar.gz .
xrdcp $tarballpath .
mv ../*.py .
echo "started extracting at $(date +%s)"
tar xf daskworkerenv.tar.*
echo "finished extracting at $(date +%s)"


source daskworkerenv/bin/activate

ls -lrth
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/daskworkerenv/bin:$PATH

export DASK_DISTRIBUTED__WORKER__MEMORY__TARGET=0.85
export DASK_DISTRIBUTED__WORKER__MEMORY__SPILL=0.90
export DASK_DISTRIBUTED__WORKER__MEMORY__PAUSE=0.95
export DASK_DISTRIBUTED__WORKER__MEMORY__TERMINATE=0.99

echo "Running command: $@"

$@
