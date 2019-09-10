import awkward
import uproot
import numpy as np
import time
import blosc
import os
import sys
from tqdm import tqdm
import argparse

# in
# ~/Library/Python/3.7/lib/python/site-packages/awkward/persist.py
# comment out 
    # if any(n.startswith(name) for n in namelist):
    #     raise KeyError("cannot add {0} to zipfile because the following already exist: {1}".format(repr(name), ", ".join(repr(n) for n in namelist if n.startswith(name))))

# sed -i 's/raise KeyError/print/' ~/Library/Python/3.7/lib/python/site-packages/awkward/persist.py

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="filename")
    args = parser.parse_args()

    f = uproot.open(str(args.filename))
    t = f["Events"]

    ac = [dict(
        types=[np.bool_,bool,np.integer,np.float32],
        pair=(lambda x: blosc.compress(x,cname="lz4hc"), ("blosc", "decompress")),
        minsize=8192,
        contexts="*",
        )]

    bnames = [bn.decode("ascii") for bn in t.keys()]

    t0 = time.time()
    fname = "table.awkd"
    for iname,name in enumerate(tqdm(bnames)):
        arr = t.array(name)
        awkward.save(fname,arr,name=name,compression=ac,mode="w" if iname==0 else "a",)
    t1 = time.time()
    print(t1-t0)
