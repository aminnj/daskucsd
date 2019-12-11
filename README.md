Getting [dask](https://distributed.dask.org/en/latest/) up and running at the local T2, based on [this](https://github.com/aminnj/redis-htcondor).

## Quick start

Clone the repository and work inside it:
```bash
git clone https://github.com/aminnj/daskucsd
cd daskucsd
```

Install conda and get all the dependencies:
```
curl -O -L https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b 

# add conda to the end of ~/.bashrc, so relogin after executing this line
~/miniconda3/bin/conda init

# stop conda from activating the base environment on login
conda config --set auto_activate_base false
conda config --add channels conda-forge

# install package to tarball environments
conda install --name base conda-pack -y

# create environments with as much stuff from anaconda
conda create --name workerenv uproot dask -y
conda create --name analysisenv uproot dask matplotlib pandas jupyter hdfs3 -y
# and then install residual packages with pip
conda run --name workerenv pip install dask-jobqueue
conda run --name analysisenv pip install jupyter-server-proxy dask-jobqueue

# make the tarball for the worker nodes
conda pack -n workerenv --arcroot workerenv -f --format tar.gz \
    --compress-level 9 -j 8 --exclude "*.pyc" --exclude "*.js.map" --exclude "*.a"
```

Start dask scheduler in a GNU screen/separate terminal:
```
( conda activate analysisenv && dask-scheduler --port 50123 )
```

Submit some workers:
```bash
python condor_utils.py -r <hostname:port of scheduler> -n 10
```

Start analysis jupyter notebook:
```bash
( conda activate analysisenv && jupyter notebook --no-browser )
```
