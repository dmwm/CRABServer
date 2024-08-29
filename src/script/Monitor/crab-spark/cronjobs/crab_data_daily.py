# import pickle
from datetime import datetime, timedelta

# import click
import os
import pandas as pd
# import pprint
import time
# from dateutil.relativedelta import relativedelta

import numpy as np
import json
import osearch

import argparse

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-s", "--start-date", default=None,
  help="process data starting from this day, inclusive (YYYY-MM-DD)",)
parser.add_argument("-e", "--end-date", default=None,
  help="process data until this day, not included (YYYY-MM-DD)",)
args = parser.parse_args()
print(f"timerange: [{args.start_date} {args.end_date})" )


from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, collect_list, concat_ws, greatest, lit, lower, when,
    avg as _avg,
    count as _count,
    hex as _hex,
    max as _max,
    min as _min,
    round as _round,
    sum as _sum,
)
from pyspark.sql.types import (
    LongType,
)
from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

# Query date

#TODAY = str(datetime.now())[:10]
#YESTERDAY = str(datetime.now()-timedelta(days=1))[:10]
#wa_date = TODAY

if args.end_date:
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
else:
    end_date = datetime.now()
    end_date = end_date.replace(minute=0, hour=0, second=0, microsecond=0)

if args.start_date:
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
else:
    start_date = end_date - timedelta(days=1)

date_list = pd.date_range(
    start=start_date,
    end=end_date,
    ).to_pydatetime().tolist()

# Import data into database form

def process_single_day(day):

    wa_date = day.strftime("%Y-%m-%d")
    TODAY = wa_date
    YESTERDAY = (day-timedelta(days=1)).strftime("%Y-%m-%d")

    HDFS_CRAB_part = f'/project/awg/cms/crab/tasks/{wa_date}/'
    print("==============================================="
          , "CRAB Table"
          , "==============================================="
          , "File Directory:", HDFS_CRAB_part
          , "Work Directory:", os.getcwd()
          , "==============================================="
          , "===============================================", sep='\n')
    
    crab_part = spark.read.format('avro').load(HDFS_CRAB_part)
    df = crab_part.select("TM_TASKNAME","TM_START_TIME","TM_TASK_STATUS","TM_SPLIT_ALGO","TM_USERNAME","TM_USER_ROLE","TM_JOB_TYPE","TM_IGNORE_LOCALITY","TM_SCRIPTEXE","TM_USER_CONFIG")
    df.createOrReplaceTempView("crab_algo")
    
    # Query daily data
    
    query = f"""\
    SELECT *
    FROM crab_algo 
    WHERE 1=1
    AND TM_START_TIME >= unix_timestamp("{YESTERDAY} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000 
    AND TM_START_TIME < unix_timestamp("{TODAY} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000 
    """
    
    tmpdf = spark.sql(query)
    tmpdf.show(10)
    
    # Convert database to dictionary
    
    docs = tmpdf.toPandas().to_dict('records')
    
    # Extract 'REQUIRE_ACCELERATOR' from 'TM_USER_CONFIG'
    
    for i in range(len(docs)):
        if docs[i]['TM_USER_CONFIG'] is not None:
            data = json.loads(docs[i]['TM_USER_CONFIG'])
            if "requireaccelerator" in data:
                docs[i]['REQUIRE_ACCELERATOR'] = data["requireaccelerator"]
            else:
                docs[i]['REQUIRE_ACCELERATOR'] = None
        else:
            docs[i]['REQUIRE_ACCELERATOR'] = None
    
    # Define type of each schema
    
    def get_index_schema():
        return {
            "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
            "mappings": {
                "properties": {
                    "TM_TASKNAME": {"ignore_above": 2048, "type": "keyword"},
                    "TM_START_TIME": {"format": "epoch_millis", "type": "date"},
                    'TM_TASK_STATUS': {"ignore_above": 2048, "type": "keyword"},
                    "TM_SPLIT_ALGO": {"ignore_above": 2048, "type": "keyword"},
                    "TM_USERNAME": {"ignore_above": 2048, "type": "keyword"},
                    "TM_USER_ROLE": {"ignore_above": 2048, "type": "keyword"},
                    "TM_JOB_TYPE": {"ignore_above": 2048, "type": "keyword"},
                    "TM_IGNORE_LOCALITY": {"ignore_above": 2048, "type": "keyword"},
                    "TM_SCRIPTEXE": {"ignore_above": 2048, "type": "keyword"},
                    "REQUIRE_ACCELERATOR": {"ignore_above": 2048, "type": "keyword"},
                }
            }
        }
    
    # Send data to Opensearch
    
    _index_template = 'crab-taskdb'
    client = osearch.get_es_client("os-cms.cern.ch/es", '/data/certs/monit.d/monit_spark_crab.txt', get_index_schema())
    idx = client.get_or_create_index(timestamp=day.strftime("%s"), index_template=_index_template, index_mod="M")
    no_of_fail_saved = client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)
    
    print("================================= CRAB Table ======================================="
          , "FINISHED : ", len(docs), "ROWS ARE SENT", no_of_fail_saved, "ROWS ARE FAILED"
          , "=================================  CRAB Table =======================================", sep='\n')


for day in date_list:
    process_single_day(day)

