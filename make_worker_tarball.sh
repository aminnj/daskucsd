#!/usr/bin/env bash

cat << EOF | singularity shell --bind /hadoop --bind /cvmfs /cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6

source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc700
cmsrel CMSSW_10_5_0
cd CMSSW_10_5_0
cmsenv
cd -

export PYTHONUSERBASE=x
python3 -m venv workerenv

. workerenv/bin/activate
pip3 install psutil dask distributed uproot backports.lzma

sed -i 's|#!.*/python|#!/usr/bin/env python|' workerenv/bin/dask-*

echo "Making workerenv.tar.xz"
tar --exclude='__pycache__' -cJf workerenv.tar.xz workerenv/
echo "Done"

python3 -c "import lzma"
python3 -c "from backports.lzma import compress; print(compress)"

EOF
