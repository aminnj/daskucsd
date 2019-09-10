#!/usr/bin/env python

import os
import tempfile
import argparse
import glob
import sys
import commands

template = """
universe                = vanilla
should_transfer_files   = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_output_files = ""
Transfer_Executable     = True
transfer_input_files    = ""
output                  = logs/1e.$(Cluster).$(Process).out
error                   = logs/1e.$(Cluster).$(Process).err
log                     = logs/$(Cluster).log
executable              = condor_executable.sh
RequestCpus = 1
RequestMemory = 4000
RequestDisk = 4000
x509userproxy={proxy}
+Owner = undefined
+project_Name = "cmssurfandturf"
+DESIRED_Sites="T2_US_UCSD"
+SingularityImage="/cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6"
JobBatchName = "converter"
Requirements = ((HAS_SINGULARITY=?=True) && (HAS_CVMFS_cms_cern_ch =?= true) && {extra_requirements})
"""
# Requirements = ((HAS_SINGULARITY=?=True) && (HAS_CVMFS_cms_cern_ch =?= true) && (regexp("el7",OSGVO_OS_KERNEL)) && {extra_requirements})

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dry_run", help="echo submit file, but don't submit", action="store_true")
    parser.add_argument("-b", "--blacklisted_machines", help="blacklisted machines", default=[
            "sdsc-49.t2.ucsd.edu",
            "sdsc-50.t2.ucsd.edu",
            "sdsc-68.t2.ucsd.edu",
            "cabinet-7-7-36.t2.ucsd.edu",
            "cabinet-8-8-1.t2.ucsd.edu",
    ], action="append")
    parser.add_argument("-w", "--whitelisted_machines", help="whitelisted machines", default=[], action="append")
    args = parser.parse_args()

    extra_requirements = "True"
    if args.blacklisted_machines:
        extra_requirements = " && ".join(map(lambda x: '(TARGET.Machine != "{0}")'.format(x),args.blacklisted_machines))
    if args.whitelisted_machines:
        extra_requirements = " || ".join(map(lambda x: '(TARGET.Machine == "{0}")'.format(x),args.whitelisted_machines))

    content = template.format(
            extra_requirements=extra_requirements,
            proxy="/tmp/x509up_u{0}".format(os.getuid()),
            )

    running = set(map(lambda x:x.split()[0].strip(),commands.getoutput("""condor_q -const '(JobStatus != 3) && (JobBatchName=="converter")' -af Args""").splitlines()))
    # running = []

    nqueued = 0
    # for inputname in glob.glob("/hadoop/cms/store/group/snt/nanoaod/DoubleMuon__Run2018A-Nano1June2019-v1/76*.root"):
    for inputname in glob.glob("/hadoop/cms/store/group/snt/nanoaod/DoubleMuon__*Nano1June2019*/*.root"):
        outputname = inputname.replace("/nanoaod/","/nanoaod/lz4/")
        if os.path.exists(outputname): continue
        if inputname in running: continue
        content += "\nArguments = {} {}\nqueue 1\n".format(inputname,outputname)
        nqueued += 1
    print("Submitting jobs for {} files".format(nqueued))

    if nqueued == 0:
        sys.exit()

    f = tempfile.NamedTemporaryFile(delete=False)
    filename = f.name
    f.write(content)
    f.close()

    if args.dry_run:
        print(content)
    else:
        os.system("mkdir -p logs/")
        # print("FIXME")
        os.system("condor_submit " + filename)

    f.unlink(filename)

