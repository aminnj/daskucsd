import functools

@functools.lru_cache(maxsize=256)
def get_chunking(filelist, chunksize, treename="Events", workers=12, skip_bad_files=False):
    """
    Return 2-tuple of
    - chunks: triplets of (filename,entrystart,entrystop) calculated with input `chunksize` and `filelist`
    - total_nevents: total event count over `filelist`
    """
    import uproot
    import awkward
    from tqdm.auto import tqdm
    import concurrent.futures
    chunksize = int(chunksize)
    chunks = []
    nevents = 0
    if skip_bad_files:
        # slightly slower (serial loop), but can skip bad files
        for fname in tqdm(filelist):
            try:
                items = uproot.numentries(fname, treename, total=False).items()
            except (IndexError, ValueError) as e:
                print("Skipping bad file", fname)
                continue
            for fn, nentries in items:
                nevents += nentries
                for index in range(nentries // chunksize + 1):
                    chunks.append((fn, chunksize*index, min(chunksize*(index+1), nentries)))
    elif filelist[0].endswith(".awkd"):
        for fname in tqdm(filelist):
            f = awkward.load(fname,whitelist=awkward.persist.whitelist + [['blosc', 'decompress']])
            nentries = len(f["run"])
            nevents += nentries
            for index in range(nentries // chunksize + 1):
                chunks.append((fname, chunksize*index, min(chunksize*(index+1), nentries)))
    else:
        executor = None if len(filelist) < 5 else concurrent.futures.ThreadPoolExecutor(min(workers, len(filelist)))
        for fn, nentries in uproot.numentries(filelist, treename, total=False, executor=executor).items():
            nevents += nentries
            for index in range(nentries // chunksize + 1):
                chunks.append((fn, chunksize*index, min(chunksize*(index+1), nentries)))
    return chunks, nevents

def bokeh_output_notebook():
    from bokeh.io import output_notebook
    output_notebook()

def plot_timeflow(taskstream):
    """
    taskstream from `client.get_task_stream(count=len(futures))`
    """
    from bokeh.io import show, output_notebook
    from bokeh.models import ColumnDataSource
    from bokeh.plotting import figure
    import pandas as pd

    df = pd.DataFrame(taskstream)
    df["tstart"] = df["startstops"].str[0].str[1]
    df["tstop"] = df["startstops"].str[0].str[2]
    df = df[["worker","tstart","tstop"]].sort_values(["worker","tstart"])
    df[["tstart","tstop"]] -= df["tstart"].min()
    df["worker"] = df["worker"].str.replace("tcp://","")

    if df["tstop"].max() > 10.: mult, unit = 1, "s"
    else: mult, unit = 1000, "ms"

    df[["tstart","tstop"]] *= mult
    df["duration"] = df["tstop"] - df["tstart"]

    group = df.groupby("worker")
    source = ColumnDataSource(group)

    wtime = (df["tstop"]-df["tstart"]).sum()
    nworkers = df["worker"].nunique()
    ttime = df["tstop"].max()*nworkers
    title = (", ".join([
        "{} workers".format(nworkers),
        "efficiency = {:.1f}%".format(100.0*wtime/ttime),
        "median task time = {:.2f}{}".format(group.apply(lambda x:x["tstop"]-x["tstart"]).median(),unit),
        "median intertask time = {:.2f}{}".format(group.apply(lambda x:x["tstart"].shift(-1)-x["tstop"]).median(),unit),
        ]))

    p = figure(y_range=group, x_range=[0,df["tstop"].max()], title=title,
               tooltips = [
                   ["worker","@worker"],
                   ["start","@{tstart}"+unit],
                   ["stop","@{tstop}"+unit],
                   ["duration","@{duration}"+unit],
               ],
              )
    p.hbar(y="worker", left="tstart", right="tstop", height=1.0, line_color="black", source=df)
    p.xaxis.axis_label = "elapsed time since start ({})".format(unit)
    p.yaxis.axis_label = "worker"
    p.plot_width = 800
    p.plot_height = 350

    try:
        show(p)
    except:
        show(p)


def plot_cumulative_events(taskstream,futures,chunks, ax=None):
    """
    taskstream from `client.get_task_stream(count=len(futures))`
    """
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    lut = dict((future.key,chunk) for future,chunk in zip(futures,chunks))
    df = pd.DataFrame(taskstream)
    df["chunk"] = df.key.map(lut)
    df["estart"] = df["chunk"].str[1]
    df["estop"] = df["chunk"].str[2]
    df["tstart"] = df["startstops"].str[0].str[1]
    df["tstop"] = df["startstops"].str[0].str[2]
    df["worker"] = df["worker"].str.replace("tcp://","")
    df["elapsed"] = df["tstop"]-df["tstart"]
    df[["tstart", "tstop"]] -= df["tstart"].min()
    df = df.sort_values("tstop")
    if ax is None:
        fig, ax = plt.subplots()
    xs, ys = df["tstop"], (df["estop"]-df["estart"]).cumsum()/1e6
    ax.plot(xs, ys)

    # https://stackoverflow.com/questions/13691775/python-pinpointing-the-linear-part-of-a-slope
    # create convolution kernel for calculating
    # the smoothed second order derivative
    smooth_width = int(len(xs)*0.5)
    x1 = np.linspace(-3, 3, smooth_width)
    norm = np.sum(np.exp(-x1**2)) * (x1[1]-x1[0])
    y1 = (4*x1**2 - 2) * np.exp(-x1**2) / smooth_width*8
    y_conv = np.convolve(ys, y1, mode="same")
    central = (np.abs(y_conv) < y_conv.std()/2.0)
    m, b = np.polyfit(xs[central], ys[central], 1)
    # fit again with points closest to the first fit
    resids = (m*xs+b)-ys
    better = (np.abs(resids-resids.mean())/resids.std() < 1.0)
    m, b = np.polyfit(xs[better & central], ys[better & central], 1)
    ax.plot(xs[better & central], m*xs[better & central] + b,
            label="fit ({:.2f}Mevents/s)".format(m))
    ax.set_xlabel("time since start [s]")
    ax.set_ylabel("cumulative Mevents")
    ax.set_title("Processed {:.2f}Mevents in {:.2f}s @ {:.3f}MHz".format(
        ys.max(), xs.max(), ys.max()/xs.max()))
    ax.legend()

def get_tree_and_branchcache(fname,cache_tree=True,cache_branches=True,xrootd=False):
    """
    return (potentially cached) uproot tree object and array/branch cache object
    works on worker and locally, though locally no caching is done
    """
    from distributed import get_worker
    islocal = False
    if xrootd:
        fname = fname.replace("/hadoop/cms","root://redirector.t2.ucsd.edu/")
    try:
        worker = get_worker()
    except ValueError:
        islocal = True
    import uproot
    cache = None
    if islocal:
        t = uproot.open(fname)["Events"]
    else:
        if not cache_tree:
            t = uproot.open(fname)["Events"]
        elif fname in worker.tree_cache:
            t = worker.tree_cache[fname]
        else:
            t = uproot.open(fname)["Events"]
            worker.tree_cache[fname] = t
        if cache_branches:
            cache = worker.array_cache
        worker.nevents += len(t)
    return t, cache
