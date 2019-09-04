#!/usr/bin/env bash

cat << EOF | singularity shell --bind /hadoop --bind /cvmfs /cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6

source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc700
cmsrel CMSSW_10_5_0
cd CMSSW_10_5_0
cmsenv
cd -

. workerenv/bin/activate

export PYTHONPATH=$(pwd):$PYTHONPATH

dask-worker uaf-1.t2.ucsd.edu:50123 --memory-limit 8GB --nprocs 1 --nthreads 1 --no-nanny

EOF

# dask-worker uaf-1.t2.ucsd.edu:50123 --memory-limit 8GB --nprocs 1 --nthreads 1
