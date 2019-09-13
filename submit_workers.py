#!/usr/bin/env python

import os
import tempfile
import argparse

template = """
universe                = vanilla
should_transfer_files   = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_output_files = ""
Transfer_Executable     = True
transfer_input_files    = utils.py,cachepreload.py,workerenv.tar.xz
output                  = logs/1e.$(Cluster).$(Process).out
error                   = logs/1e.$(Cluster).$(Process).err
log                     = logs/$(Cluster).log
executable              = condor_executable.sh
RequestCpus = 1
RequestMemory = 6000
RequestDisk = 6000
+DESIRED_Sites="T2_US_UCSD"
+SingularityImage="/cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6"
JobBatchName = "daskworker"
Requirements = ((HAS_SINGULARITY=?=True) && (HAS_CVMFS_cms_cern_ch =?= true) && {extra_requirements})
Arguments = {scheduler_url}
queue {num_workers}
"""
# Requirements = ((HAS_SINGULARITY=?=True) && (HAS_CVMFS_cms_cern_ch =?= true) && (regexp("el7",OSGVO_OS_KERNEL)) && {extra_requirements})

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--scheduler_url", help="scheduler url"
            "(if not specified, attempts to use the result of `from config import scheduler_URL`)", default="")
    parser.add_argument("-d", "--dry_run", help="echo submit file, but don't submit", action="store_true")
    parser.add_argument("-n", "--num_workers", help="number of workers", default=1, type=int)
    parser.add_argument("-b", "--blacklisted_machines", help="blacklisted machines", default=[
            "sdsc-49.t2.ucsd.edu",
            "sdsc-50.t2.ucsd.edu",
            "sdsc-68.t2.ucsd.edu",
            "cabinet-7-7-36.t2.ucsd.edu",
            "cabinet-8-8-1.t2.ucsd.edu",
    ], action="append")
    parser.add_argument("-w", "--whitelisted_machines", help="whitelisted machines", default=[], action="append")
    args = parser.parse_args()

    scheduler_url = args.scheduler_url
    if not scheduler_url:
        try:
            from config import SCHEDULER_URL as default_scheduler_url
            scheduler_url = default_scheduler_url
        except ImportError as e:
            raise Exception("You didn't specify a scheduler url, and I couldn't find one in config.SCHEDULER_URL")


    extra_requirements = "True"
    if args.blacklisted_machines:
        extra_requirements = " && ".join(map(lambda x: '(TARGET.Machine != "{0}")'.format(x),args.blacklisted_machines))
    if args.whitelisted_machines:
        extra_requirements = " || ".join(map(lambda x: '(TARGET.Machine == "{0}")'.format(x),args.whitelisted_machines))

    content = template.format(
            extra_requirements=extra_requirements,
            num_workers=args.num_workers,
            scheduler_url=scheduler_url,
            )

    f = tempfile.NamedTemporaryFile(delete=False)
    filename = f.name
    f.write(content)
    f.close()

    if args.dry_run:
        print(content)
    else:
        os.system("mkdir -p logs/")
        os.system("condor_submit " + filename)

    f.unlink(filename)

