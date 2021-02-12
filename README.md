Getting [dask](https://distributed.dask.org/en/latest/) up and running at the local T2, based on [this](https://github.com/aminnj/redis-htcondor).

## "Quick" start

### Installation

Clone the repository and work inside it:
```bash
git clone https://github.com/aminnj/daskucsd
cd daskucsd
```

Install conda and get all the dependencies:
```bash
curl -O -L https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b 

# add conda to the end of ~/.bashrc, so relogin after executing this line
~/miniconda3/bin/conda init

# stop conda from activating the base environment on login
conda config --set auto_activate_base false
conda config --add channels conda-forge

# install package to tarball environments
conda install --name base conda-pack -y

WORKERENVNAME="daskworkerenv"
ANALYSISENVNAME="daskanalysisenv"

# create environments with as much stuff from anaconda
# ipython==7.10.1 because of https://stackoverflow.com/questions/63413807/deprecation-warning-from-jupyter-should-run-async-will-not-call-transform-c
# dask>=2020 failing for some reason
packages="uproot dask<=2.30.1 dask-jobqueue==0.7.2 matplotlib pandas jupyter pyarrow fastparquet numba numexpr bottleneck ipython<=7.10.1"
conda create --name $WORKERENVNAME $packages -y
conda create --name $ANALYSISENVNAME $packages -y

# and then install residual packages with pip
conda run --name $WORKERENVNAME pip install yahist coffea awkward0 uproot3 pdroot
conda run --name $ANALYSISENVNAME pip install yahist jupyter-server-proxy coffea jupyter_nbextensions_configurator awkward0 uproot3 pdroot

# make the tarball for the worker nodes
conda pack -n $WORKERENVNAME --arcroot daskworkerenv -f --format tar.gz \
    --compress-level 9 -j 8 --exclude "*.pyc" --exclude "*.js.map" --exclude "*.a" --exclude "*pandoc"

# if that command errors (conflicting awkward/uproot versions between conda install and pip install), then do
#     conda install --name $WORKERENVNAME uproot --force-reinstall --update-deps
#     conda install --name $ANALYSISENVNAME uproot --force-reinstall --update-deps
# and then repeat the command

# if not copied to this exact directory, that's fine, but you will need to then specify
# the `tarballpath` kwarg for `make_htcondor_cluster()`
mkdir -p /hadoop/cms/store/user/$USER/daskenvs/
cp ${WORKERENVNAME}.tar.gz /hadoop/cms/store/user/$USER/daskenvs/daskworkerenv.tar.gz
```


### Do some analysis

Need a scheduler and a set of workers. You can either set up do this manually 
with some bash processes, or automatically within a jupyter notebook.

#### Automatically

```bash
conda activate $ANALYSISENVNAME
jupyter notebook --no-browser
```
and then run `cluster.ipynb`.

#### Manually

```bash
# start dask scheduler in a GNU screen/separate terminal
( conda activate analysisenv && dask-scheduler --port 50123 )
# submit some workers
conda activate analysisenv
python condor_utils.py -r "$(hostname):50123" -n 10
# start analysis jupyter notebook
jupyter notebook --no-browser
```

## Misc notes

To forward port locally:
```
ps aux | grep "localhost:$PORT" | grep -v "grep" | awk '{print $2}' | xargs kill -9
ssh -N -f -L localhost:$PORT:localhost:$PORT $HOST
```

