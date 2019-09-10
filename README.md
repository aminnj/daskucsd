Getting [dask](https://distributed.dask.org/en/latest/) up and running at the local T2, based on [this](https://github.com/aminnj/redis-htcondor).

## Quick start

Clone the repository and work inside it:
```bash
git clone https://github.com/aminnj/daskucsd
cd daskucsd
```

Make worker environment
```bash
./make_worker_tarball.sh
```

Submit some workers
```bash
python submit_workers.py -n 10
```

Start analysis jupyter notebook
```bash
./start_analysis_server.sh
```
For testing without condor, use `start_dask_scheduler.sh` and `start_dask_worker.sh`.

