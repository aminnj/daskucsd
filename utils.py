import functools
import time
import uproot3
import uproot4
from tqdm.auto import tqdm
import concurrent.futures
from dask.distributed import as_completed, get_client, get_worker
from collections import defaultdict
import pdroot

@functools.lru_cache(maxsize=256)
def get_chunking(filelist, chunksize, treename="Events", workers=12, skip_bad_files=False, xrootd=False, client=None, use_dask=False):
    """
    Return 2-tuple of
    - chunks: triplets of (filename,entrystart,entrystop) calculated with input `chunksize` and `filelist`
    - total_nevents: total event count over `filelist`
    """

    if xrootd:
        temp = []
        for fname in filelist:
            if fname.startswith("/hadoop/cms"):
                temp.append(fname.replace("/hadoop/cms","root://redirector.t2.ucsd.edu/"))
            else:
                temp.append(fname.replace("/store/","root://xrootd.t2.ucsd.edu:2040//store/"))
        filelist = temp

    chunksize = int(chunksize)
    chunks = []
    nevents = 0
    
    if use_dask:
        if not client:
            client = get_client()
        def numentries(fname):
            try:
                return (fname,uproot3.numentries(fname,treename))
            except:
                return (fname,-1)
        futures = client.map(numentries, filelist)
        info = []
        for future, result in tqdm(as_completed(futures, with_results=True), total=len(futures)):
            info.append(result)
        for fn, nentries in info:
            if nentries < 0:
                if skip_bad_files:
                    print("Skipping bad file: {}".format(fn))
                    continue
                else: raise RuntimeError("Bad file: {}".format(fn))
            nevents += nentries
            for index in range(nentries // chunksize + 1):
                chunks.append((fn, chunksize*index, min(chunksize*(index+1), nentries)))
    else:
        if skip_bad_files:
            # slightly slower (serial loop), but can skip bad files
            for fname in tqdm(filelist):
                try:
                    items = uproot3.numentries(fname, treename, total=False).items()
                except (IndexError, ValueError) as e:
                    print("Skipping bad file", fname)
                    continue
                for fn, nentries in items:
                    nevents += nentries
                    for index in range(nentries // chunksize + 1):
                        chunks.append((fn, chunksize*index, min(chunksize*(index+1), nentries)))
        else:
            executor = None if len(filelist) < 5 else concurrent.futures.ThreadPoolExecutor(min(workers, len(filelist)))
            for fn, nentries in uproot3.numentries(filelist, treename, total=False, executor=executor).items():
                nevents += nentries
                for index in range(nentries // chunksize + 1):
                    chunks.append((fn, chunksize*index, min(chunksize*(index+1), nentries)))

    return chunks, nevents


def combine_dicts(dicts):
    new_dict = dict()
    for d in dicts:
        for k,v in d.items():
            if k not in new_dict:
                new_dict[k] = v
            else:
                new_dict[k] += v
    return new_dict

def clear_tree_cache(client=None):
    if not client:
        client = get_client()
    def f():
        worker = get_worker()
        if hasattr(worker, "tree_cache"):
            worker.tree_cache.clear()
    client.run(f)


def get_results(func, fnames, chunksize=250e3, client=None, use_tree_cache=False, skip_bad_files=False, skip_tail_fraction=1.0, wrap_func=True):
    if not client:
        client = get_client()
    print("Making chunks for workers")
    chunks, nevents_total = get_chunking(tuple(fnames), chunksize=chunksize, use_dask=True, skip_bad_files=skip_bad_files)
    print(f"Processing {len(chunks)} chunks")
    if wrap_func:
        process = use_chunk_input(func, use_tree_cache=use_tree_cache)
    else:
        process = func

    
    chunk_workers = None
    if use_tree_cache:
        def f():
            worker = get_worker()
            return list(worker.tree_cache.keys())
        filename_to_worker = defaultdict(list)
        for worker, filenames in client.run(f).items():
            for filename in filenames:
                filename_to_worker[filename].append(worker)
        chunk_workers = [filename_to_worker[chunk[0]] for chunk in chunks]

    futures = client.map(process, chunks, workers=chunk_workers)
    t0 = time.time()
    bar = tqdm(total=nevents_total, unit="events", unit_scale=True)
    ac = as_completed(futures, with_results=True)
    results = []
    # for future, result in ac:
    #     results.append(result)
    #     bar.update(result["nevents_processed"])
    #     if (skip_tail_fraction < 1.0) and (1.0*len(results)/len(futures) >= skip_tail_fraction):
    #         print(f"Reached {100*skip_tail_fraction:.1f}% completion. Ignoring tail tasks")
    #         break
    for batch in ac.batches():
        to_break = False
        for future, result in batch:
            results.append(result)
            bar.update(result["nevents_processed"])
            if (skip_tail_fraction < 1.0) and (1.0*len(results)/len(futures) >= skip_tail_fraction):
                print(f"Reached {100*skip_tail_fraction:.1f}% completion. Ignoring tail tasks")
                to_break = True
                break
        if to_break:
            break
    bar.close()
    t1 = time.time()
    results = combine_dicts(results)
    client.cancel(futures, force=True)
    # list(map(lambda x: x.cancel(), futures))
    # del futures
    nevents_processed = results["nevents_processed"]
    print(f"Processed {nevents_processed:.5g} input events in {t1-t0:.1f}s ({1.0e-3*nevents_processed/(t1-t0):.2f}kHz)")
    return results


class DataFrameWrapper(object):
    def __init__(self, filename, entry_start=None, entry_stop=None, treename="Events", use_tree_cache=False):
        self.filename = filename
        self.entry_start = entry_start
        self.entry_stop = entry_stop
        self.treename = treename
        self.data = dict()
        
        cache = None
        try:
            from dask.distributed import get_worker
            worker = get_worker()
            if use_tree_cache and hasattr(worker, "tree_cache"):
                cache = worker.tree_cache
        except:
            pass
        
        if cache is not None:
            if filename not in cache:
                cache[filename] = uproot4.open(filename)[treename]
            self.t = cache[filename]
        else:
            self.t = uproot4.open(filename)[treename]

    def __getitem__(self, key):
        if key not in self.data:
            self.data[key] = self.t.get(key).array(entry_start=self.entry_start, entry_stop=self.entry_stop)
        return self.data[key]

    def __len__(self):
        if None not in [self.entry_start, self.entry_stop]:
            return self.entry_stop-self.entry_start
        return len(self.t)

def use_chunk_input(func, **kwargs):
    def wrapper(chunk):
        # df = DataFrameWrapper(*chunk, **kwargs)
        fname, entry_start, entry_stop = chunk
        df = pdroot.ChunkDataFrame(filename=fname, entry_start=entry_start, entry_stop=entry_stop)
        t0 = time.time()
        out = func(df)
        t1 = time.time()
        out["nevents_processed"] = len(df)
        out["t_start"] = [t0]
        out["t_stop"] = [t1]
        try:
            out["worker_name"] = [get_worker().address]
        except:
            out["worker_name"] = ["local"]
        return out
    return wrapper

def plot_timeflow(results):
    from bokeh.io import show, output_notebook
    from bokeh.models import ColumnDataSource
    from bokeh.plotting import figure
    import pandas as pd

    output_notebook()

    df = pd.DataFrame()
    df["worker"] = results["worker_name"]
    df["tstart"] = results["t_start"]
    df["tstop"] = results["t_stop"]
    df[["tstart","tstop"]] -= df["tstart"].min()
    df["worker"] = df["worker"].astype("category").cat.codes

    mult, unit = 1, "s"

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

    p = figure(
        title=title,
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
    p.plot_width = 600
    p.plot_height = 300

    show(p)


def monitor_and_kill_stuck_workers(threshold=90., dryrun=False):
    from IPython.display import clear_output, display
    import ipywidgets
    import datetime
    import threading
    import time

    output = ipywidgets.Output()
    display(output)

    def monitor():

        monitor_interval = 5.0 # seconds

        nloops = 100000 # make 100000 eventually to never end

        # (worker, taskid) -> [tstart, tstop]
        runtimes_old = dict()
        runtimes_new = dict()

        output.append_stdout(f"Looking for workers that are taking more than {threshold:.1f}s per task, and killing them.")

        for _ in range(nloops):

            t = time.time()
            for worker,taskids in client.processing().items():
                # only care about first task in lineup
                for taskid in taskids[:1]:
                    key = (worker, taskid)
                    tstart = tstop = t
                    if key in runtimes_old:
                        tstart = runtimes_old[key][0]
                    runtimes_new[key] = [tstart, tstop]

            workers_to_kill = []
            for (worker,taskid),(tstart,tstop) in runtimes_new.items():
                dt = tstop - tstart
                if dt >= threshold:
                    now = datetime.datetime.now()
                    output.append_stdout(f"[{now}] Killing worker {worker} (stuck for {dt:.1f}s)\n")
                    workers_to_kill.append(worker)
            if not dryrun:
                client.retire_workers(workers_to_kill)

            runtimes_old, runtimes_new = dict(runtimes_new), dict()

            time.sleep(monitor_interval)

    thread = threading.Thread(target=monitor)
    thread.start()
    return thread
