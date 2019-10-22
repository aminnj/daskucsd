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
transfer_input_files    = utils.py,cachepreload.py,workerenv.tar.gz
output                  = logs/1e.$(Cluster).$(Process).out
error                   = logs/1e.$(Cluster).$(Process).err
log                     = logs/$(Cluster).log
executable              = condor_executable.sh
RequestCpus = 1
RequestMemory = {memory}
RequestDisk = {disk}
x509userproxy={proxy}
+DESIRED_Sites="T2_US_UCSD"
+SingularityImage="/cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel6"
JobBatchName = "daskworker"
Requirements = ((HAS_SINGULARITY=?=True) && (HAS_CVMFS_cms_cern_ch =?= true) && {extra_requirements})
Arguments = {scheduler_url}
queue {num_workers}
"""

def submit_workers(scheduler_url, dry_run=False, num_workers=1, blacklisted_machines=[
            "sdsc-49.t2.ucsd.edu",
            "sdsc-50.t2.ucsd.edu",
            "sdsc-68.t2.ucsd.edu",
            "cabinet-7-7-36.t2.ucsd.edu",
            "cabinet-8-8-1.t2.ucsd.edu",
            "cabinet-4-4-18.t2.ucsd.edu",
    ], memory=4000, disk=20000, whitelisted_machines=[]):

    if not scheduler_url:
        try:
            from config import SCHEDULER_URL as default_scheduler_url
            scheduler_url = default_scheduler_url
        except ImportError as e:
            raise Exception("You didn't specify a scheduler url, and I couldn't find one in config.SCHEDULER_URL")


    extra_requirements = "True"
    if blacklisted_machines:
        extra_requirements = " && ".join(map(lambda x: '(TARGET.Machine != "{0}")'.format(x),blacklisted_machines))
    if whitelisted_machines:
        extra_requirements = " || ".join(map(lambda x: '(TARGET.Machine == "{0}")'.format(x),whitelisted_machines))

    content = template.format(
            extra_requirements=extra_requirements,
            num_workers=num_workers,
            scheduler_url=scheduler_url,
            proxy="/tmp/x509up_u{0}".format(os.getuid()),
            memory=memory,
            disk=disk,
            )

    f = tempfile.NamedTemporaryFile(delete=False)
    filename = f.name
    f.write(content.encode())
    f.close()

    if dry_run:
        print(content)
    else:
        os.system("mkdir -p logs/")
        os.system("condor_submit " + filename)

    # f.unlink(filename)

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

    submit_workers(args.scheduler_url, dry_run=args.dry_run, num_workers=args.num_workers, blacklisted_machines=args.blacklisted_machines, whitelisted_machines=args.whitelisted_machines)
