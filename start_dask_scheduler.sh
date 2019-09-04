#!/usr/bin/env bash

cat << EOF | singularity shell --bind /hadoop --bind /cvmfs /cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6

source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc700
cmsrel CMSSW_10_5_0
cd CMSSW_10_5_0
cmsenv
cd -

. analysisenv/bin/activate

export PYTHONPATH=$(pwd):$PYTHONPATH

dask-scheduler --dashboard --show --port 50123

EOF
