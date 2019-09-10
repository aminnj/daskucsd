#!/usr/bin/env bash

cat << EOF | singularity shell --bind /hadoop --bind /cvmfs /cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6

source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc700
cmsrel CMSSW_10_5_0
cd CMSSW_10_5_0
cmsenv
cd -

. workerenv/bin/activate

python3 convert_awkward.py /hadoop/cms/store/group/snt/nanoaod/DoubleMuon__Run2016B_ver2-Nano1June2019_ver2-v1/A21F0244-F4B9-F44D-A47D-82F7F877C481.root

EOF
