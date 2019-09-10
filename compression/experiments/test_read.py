


import awkward
import uproot
import copy
import numpy as np
import time
import functools
import lz4.block
import lz4.frame
import blosc
import os
import sys
import pandas as pd
from tqdm.auto import tqdm

if __name__ == "__main__":

    os.system("mkdir -p rootjsons/")


    data = []

    label = "zlib"
    for label in [
            "lz4_1",
            "lz4_2",
            "lz4_3",
            "lz4_4",
            "lz4_5",
            "lz4_6",
            "lz4_7",
            "lz4_8",
            "lz4_9",
        # "lzma",
        # "uncomp",
        # "uncomp_reopt",
        # "zlib_reopt",
        # "lz4",
        # "lz4_reopt",
        # "zlib",
        ][::-1]:
        print(label)

        fname = "doublemu_{}.root".format(label)
        f = uproot.open(fname)
        t = f["Events"]

        for nbranches in range(1,12+1):
        # for nbranches in range(1,2):
            print(nbranches)

            s = "/Muon_({})/".format("|".join("pt|eta|phi|dxy|dxyErr|dz|dzErr|charge|mass|jetIdx|isGlobal|charge".split("|")[:nbranches]))
            # warm up cache
            raw = t.arrays([s],outputtype=dict,namedecode="ascii")
            times = []
            for _ in range(10):
                t0 = time.time()
                raw = t.arrays([s],outputtype=dict,namedecode="ascii")
                t1 = time.time()
                times.append(t1-t0)

            info = {}
            info["label"] = label
            info["nbranches"] = nbranches
            info["times"] = times
            data.append(info)

    pd.DataFrame(data).to_json("rootjsons/data_lz4_rev.json")

