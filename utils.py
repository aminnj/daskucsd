import functools

@functools.lru_cache(maxsize=256)
def get_chunking(filelist, chunksize, treename="Events", workers=12, skip_bad_files=False):
    """
    Return 2-tuple of
    - chunks: triplets of (filename,entrystart,entrystop) calculated with input `chunksize` and `filelist`
    - total_nevents: total event count over `filelist`
    """
    import uproot
    from tqdm.auto import tqdm
    import concurrent.futures
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

def plot_timeflow(tsdata):
    from bokeh.io import show, output_notebook
    from bokeh.models import ColumnDataSource
    from bokeh.plotting import figure
    import pandas as pd

    df = pd.DataFrame(tsdata)
    df["tstart"] = df["startstops"].str[0].str[1]
    df["tstop"] = df["startstops"].str[0].str[2]
    df = df[["worker","tstart","tstop"]].sort_values(["worker","tstart"])
    df[["tstart","tstop"]] -= df["tstart"].min()
    df["duration"] = df["tstop"] - df["tstart"]
    df["worker"] = df["worker"].str.replace("tcp://","")

    if df["tstop"].max() > 10.: mult, unit = 1, "s"
    else: mult, unit = 1000, "ms"

    df[["tstart","tstop"]] *= mult

    group = df.groupby("worker")
    source = ColumnDataSource(group)

    wtime = (df["tstop"]-df["tstart"]).sum()
    ttime = df["tstop"].max()*df["worker"].nunique()
    title = (", ".join([
        "efficiency (filled/total) = {:.1f}%".format(100.0*wtime/ttime),
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
