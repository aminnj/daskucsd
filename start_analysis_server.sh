#!/usr/bin/env bash

cat << EOF | singularity shell --bind /hadoop --bind /cvmfs /cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6

source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc700
cmsrel CMSSW_10_5_0
cd CMSSW_10_5_0
cmsenv
cd -

export PYTHONUSERBASE=x
[ -d analysisenv ] || {
    python3 -m venv analysisenv
    . analysisenv/bin/activate
    pip3 install dask distributed uproot matplotlib coffea jupyter tqdm pandas lz4 cloudpickle bokeh jupyter-server-proxy backports.lzma --ignore-installed
    rm -rf analysisenv/lib/python3.6/site-packages/psutil-5.6.3.dist-info/
}

. analysisenv/bin/activate

jupyter notebook --no-browser --port=8896

EOF

# go back to cmssw psutil to prevent so file error
# so we delete the psutil package after pip
