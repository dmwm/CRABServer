
from datetime import datetime, timedelta
import os
import pandas as pd
import time

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
import numpy as np
import osearch
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

# Data date

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

def process_single_day(day):

    wa_date = day.strftime("%Y-%m-%d")
    
    # Import data into database form
    
    HDFS_RUCIO_DATASET_LOCKS = f'/project/awg/cms/rucio/{wa_date}/dataset_locks/part*.avro'
    HDFS_RUCIO_RSES =          f'/project/awg/cms/rucio/{wa_date}/rses/part*.avro'
    HDFS_RUCIO_RULES =         f'/project/awg/cms/rucio/{wa_date}/rules'
    print("===============================================", "File Directory:", HDFS_RUCIO_DATASET_LOCKS, "Work Directory:", os.getcwd(), "===============================================", sep='\n')
    
    print("==============================================="
          , "RUCIO : Rules, RSEs, Dataset"
          , "==============================================="
          , "File Directory:", HDFS_RUCIO_DATASET_LOCKS
          , "Work Directory:", os.getcwd()
          , "==============================================="
          , "===============================================", sep='\n')
    rucio_dataset_locks = spark.read.format('avro').load(HDFS_RUCIO_DATASET_LOCKS)\
        .withColumn('BYTES', col('BYTES').cast(LongType()))\
        .withColumn('RULE_ID', lower(_hex(col('RULE_ID'))))\
        .withColumn('RSE_ID', lower(_hex(col('RSE_ID'))))
    rucio_dataset_locks.createOrReplaceTempView("dataset_locks")
    
    rucio_rses = spark.read.format('avro').load(HDFS_RUCIO_RSES)\
        .withColumn('ID', lower(_hex(col('ID'))))
    rucio_rses.createOrReplaceTempView("rses")
    
    rucio_rules = spark.read.format('avro').load(HDFS_RUCIO_RULES)\
        .withColumn('ID', lower(_hex(col('ID'))))
    rucio_rules.createOrReplaceTempView("rules")
    
    # filter and query
    
    rucio_dataset_locks = rucio_dataset_locks.filter(f"""ACCOUNT IN ('crab_tape_recall')""").cache()
    rucio_rses = rucio_rses.select('ID', 'RSE', 'RSE_TYPE').cache()
    rucio_rules = rucio_rules.select('ID', 'ACCOUNT', 'DID_TYPE', 'EXPIRES_AT').cache()
    
    result_df = rucio_dataset_locks.join(rucio_rses, rucio_rses["ID"] == rucio_dataset_locks["RSE_ID"])\
            .join(rucio_rules, rucio_rules["ID"] == rucio_dataset_locks["RULE_ID"]).drop('ID', 'RULE_ID', 'RSE_ID', 'ACCESSED_AT', 'ACCOUNT')
    
    # Convert database to dictionary
    
    docs = result_df.toPandas().to_dict('records')
    
    # Add TIMESTAMP column and convert TiB
    TIME = datetime.strptime(f"""{wa_date} 00:00:00""", "%Y-%m-%d %H:%M:%S").timestamp()*1000
    for i in range(len(docs)):
        docs[i]['TIMESTAMP'] = TIME
        docs[i]['SIZE_TiB'] = docs[i]["BYTES"]/1099511627776
        del docs[i]["BYTES"]
    
        # break down the name
        NAME_i = docs[i]['NAME']
        split_NAME = NAME_i.split('#')[0]
        docs[i]['NAME_'] = NAME_i.split('#')[0]
        split_NAME = docs[i]['NAME_'].split('/')
        if len(split_NAME) != 4:
            print("YO HOO !!, something wrong.", NAME_i)
        docs[i]['PriDataset'] = split_NAME[1]
        docs[i]['DataTier'] = split_NAME[-1]
    
    # Define type of each schema
    
    def get_index_schema():
        return {
            "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
            "mappings": {
                "properties": {
                    'SCOPE': {"ignore_above": 2048, "type": "keyword"},
                    'NAME': {"ignore_above": 2048, "type": "keyword"},
                    'STATE': {"ignore_above": 1024, "type": "keyword"},
                    'LENGTH': {"ignore_above": 1024, "type": "keyword"},
                    'SIZE_TiB': {"type": "long"},
                    'UPDATED_AT': {"format": "epoch_millis", "type": "date"},
                    'CREATED_AT': {"format": "epoch_millis", "type": "date"},
                    'RSE': {"ignore_above": 2048, "type": "keyword"},
                    'RSE_TYPE': {"ignore_above": 2048, "type": "keyword"},
                    'DID_TYPE': {"ignore_above": 1024, "type": "keyword"},
                    'EXPIRES_AT': {"format": "epoch_millis", "type": "date"},
                    'TIMESTAMP': {"format": "epoch_millis", "type": "date"},
                    'NAME_': {"ignore_above": 2048, "type": "keyword"},
                    'PriDataset': {"ignore_above": 2048, "type": "keyword"},
                    'DataTier': {"ignore_above": 2048, "type": "keyword"},
                }
            }
        }
        
    # Send data to Opensearch
    
    _index_template = 'crab-tape-recall-rules'
    client = osearch.get_es_client("os-cms.cern.ch/es", '/data/certs/monit.d/monit_spark_crab.txt', get_index_schema())
    idx = client.get_or_create_index(timestamp=day.strftime("%s"), index_template=_index_template, index_mod="M")
    no_of_fail_saved = client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)
    
    print("==================================== RUCIO : Rules, RSEs, Dataset ===================================="
          , "FINISHED : "
          , len(docs), "ROWS ARE SENT"
          , no_of_fail_saved, "ROWS ARE FAILED"
          , "==================================== RUCIO : Rules, RSEs, Dataset ====================================", sep='\n')


for day in date_list:
    process_single_day(day)

