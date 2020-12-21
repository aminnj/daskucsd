def set_dask_config():
    import dask
    dask.config.set({'distributed.worker.memory.target': 0.85,
                     'distributed.worker.memory.spill': 0.90,
                     'distributed.worker.memory.pause': 0.95,
                     'distributed.worker.memory.terminate': 0.99})

def dask_setup(worker):
    import os
    from uproot4.cache import LRUCache

    set_dask_config()

    def get_classads():
        fname = os.getenv("_CONDOR_JOB_AD")
        if not fname:
            return {}
        d = {}
        with open(fname) as fh:
            for line in fh:
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                d[k.strip()] = v.strip().lstrip('"').strip('"')
        return d
    worker.classads = get_classads()
    worker.tree_cache = LRUCache(75)

    def numtreescached_metric(worker):
        if hasattr(worker,"tree_cache"):
            return len(list(worker.tree_cache.keys()))
        return 0

    worker.metrics["numtreescached"] = numtreescached_metric

    try:
        # Load some imports initially
        import coffea.processor
        import coffea.executor
    except:
        pass
