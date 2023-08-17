# CRAB Data Pipeline

The environment to run the scripts contained here is available on crab-dev-tw04. See crab docs "cms-spark.md" for details.

---

(migrated to crab docs)

The **crab_cronjob** is material for excuting schedule datapipeline. The goal is to update wanted data on dashboards(Opensearch and Grafana) daily.

## Material
The folder contains code for pulling the data today from HDFS and upload to dashboards, where
### Data pulling
**crab_tape_recall_daily.py** - Pulls wanted data from `rucio rules_history`, to es-index: `crab-tape-recall-daily-ekong*` \
**crab_data_daily.py** - Pulls wanted data from `crab tasks`, to es-index: `crab-data-ekong*` \
**crab_rules_tape_recall_daily.py** - Pulls wanted data from rucio `dataset_locks`, `rses`, and `rules`, to es-index: `crab-tape-recall-rules-ekong*` \
**crab_condor_daily.py** - Pulls wanted data from `condor raw metric` and `crab tasks`, to es-index: `crab-condor-ekong*`
### Tools
**run_spark.sh** - Shell script for `source` environment and run `spark-submit` for `$1` file, where `$1` in the script supposes to be a data pulling file (.py) \
**cron_daily.sh** - Shell script for running **run_spark.sh** in the docker container, for all four data pulling files. Note that the file directory here must be directory in the container, not the host. \
**osearch.py** and **secret_opensearch.txt** - Tools for send data to Opensearch Dashboards, just have it in the same folder of data pulling files (.py)

## Dashboard 
Grafana: https://monit-grafana.cern.ch/goto/STBZ3uCVz?orgId=11 \
This dashboard is created to answer the following question: \
**Rucio**
1. How long do tasks stay in “taperecall”?
2. How big (TiB) are the datasets that are recalled from tape?

**CRAB Server**
1. How many tasks are using each crab features? (Split algorithm, Ignorelocality, ScriptExe, GPU)
2. How many tasks each users submit? 
3. How many jobs use ignorelocality?

**Condor Matric**
1. What is wall clock time spent by each CMS data tier and each job type?
2. What is the success rate of the Analysis job type?

## Running Daily
To upload all of the data daily, just setup crontab to run **cron_daily.sh** script everyday, for example: 
```
34 05 * * * /home/crab3/workdir/crab-dp3/crab_cronjob/cron_daily.sh &> /home/crab3/workdir/crab-dp3/crab_cronjob/cron_daily.log
```

## Debugging and recalculation
Since there is no method to delete specific data in Opensearch, when error occurs in dataframe such as data duplication or wrong schema is uploaded, you have to delete entire data, and upload them again from the beginning, manually. To do so,
- Delete the data in particular index by running `DELETE /<es index>` in Dev Tools, in `es-cms.cern.ch/`
- Use material in **crab_manual**, which provides the same functions as **crab_cronjob**. You can upload any wanted data to the any time range by changing only `start_date` and `end_date` in the particular data pulling file.
### Note
By uploading data for one week, it should take time around 2 minutes or less for each data source(1 data pulling file), except crab_condor.ipynb which spends around 5-10 minutes with high probability of failure, due to complexity and size of the data type in limited shared computing source.
