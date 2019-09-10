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
import backports.lzma
from tqdm.auto import tqdm

if __name__ == "__main__":

    os.system("mkdir -p tables/")
    os.system("mkdir -p jsons/")

    f = uproot.open("doublemu_lzma.root")
    t = f["Events"]

    raw = t.arrays(["/Muon_(pt|eta|phi|mass|charge)/"],outputtype=dict,namedecode="ascii")
    table = awkward.Table(raw)
    print(table.nbytes/1e6)


    for i in range(20):

        data = []

        for fcomp,fdecomp,label in tqdm([
            [lambda x: backports.lzma.compress(x), backports.lzma.decompress, "lzma"],
            [lambda x: blosc.compress(x), blosc.decompress, "blosc"], # default should be blosc.SHUFFLE
            [lambda x: blosc.compress(x,shuffle=blosc.NOSHUFFLE), blosc.decompress, "blosc_noshuffle"],
            [lambda x: blosc.compress(x,shuffle=blosc.SHUFFLE), blosc.decompress,"blosc_shuffle"],
            [lambda x: blosc.compress(x,shuffle=blosc.BITSHUFFLE), blosc.decompress,"blosc_bitshuffle"],
            [lambda x: blosc.compress(x,cname="zlib"), blosc.decompress,"blosc_zlib"],
            [lambda x: blosc.compress(x,cname="zlib",shuffle=blosc.NOSHUFFLE), blosc.decompress,"blosc_zlib_noshuffle"],
            [lambda x: blosc.compress(x,cname="lz4"), blosc.decompress,"blosc_lz4"],
            [lambda x: blosc.compress(x,cname="lz4",shuffle=blosc.NOSHUFFLE), blosc.decompress,"blosc_lz4_noshuffle"],
            [lambda x: blosc.compress(x,cname="lz4hc"), blosc.decompress,"blosc_lz4hc"],
            [lambda x: lz4.frame.compress(x,compression_level=lz4.frame.COMPRESSIONLEVEL_MAX),lz4.frame.decompress,"lz4_max"],
            [lambda x: lz4.frame.compress(x,compression_level=lz4.frame.COMPRESSIONLEVEL_MIN),lz4.frame.decompress,"lz4_min"],
            [lambda x: lz4.frame.compress(x,compression_level=lz4.frame.COMPRESSIONLEVEL_MINHC),lz4.frame.decompress,"lz4_minhc"],
            ]):

            info = {}
            info["label"] = label
            info["iter"] = i

            ac = copy.deepcopy(awkward.persist.compression)
            ac[0]["types"] += [np.float32]
            if label.startswith("lz4"):
                ac[0]["pair"] = (fcomp, ("lz4.frame", "decompress"))
            if label.startswith("blosc"):
                ac[0]["pair"] = (fcomp, ("blosc", "decompress"))
            if label.startswith("lzma"):
                ac[0]["pair"] = (fcomp, ("backports.lzma", "decompress"))

            fname = "tables/table_{}.awkd".format(label)

            t0 = time.time()
            awkward.save(fname,table,compression=ac,mode="w")
            t1 = time.time()
            info["t_compress_ms"] = 1e3*(t1-t0)

            t0 = time.time()
            tmp = awkward.load(fname,
                    whitelist = awkward.persist.whitelist + [
                        ['lz4.frame', 'decompress'],
                        ['lz4.block', 'decompress'],
                        ['blosc', 'decompress'],
                        ['backports.lzma', 'decompress'],
                        ]
                    )
            t1 = time.time()
            info["t_decompress_ms"] = 1e3*(t1-t0)

            info["uncompressed_bytes"] = table.nbytes
            info["compressed_bytes"] = int(os.stat(fname).st_size)

            data.append(info)

        pd.DataFrame(data).to_json("jsons/data_{}.json".format(i))

