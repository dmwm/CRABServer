
import os
import time
from datetime import datetime, timedelta

import numpy as np
import osearch
import pandas as pd
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg as _avg
from pyspark.sql.functions import col, collect_list, concat_ws
from pyspark.sql.functions import count as _count
from pyspark.sql.functions import greatest
from pyspark.sql.functions import hex as _hex
from pyspark.sql.functions import lit, lower
from pyspark.sql.functions import max as _max
from pyspark.sql.functions import min as _min
from pyspark.sql.functions import round as _round
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when
from pyspark.sql.types import LongType

spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

# Data date

TODAY = str(datetime.now())[:10]
wa_date = TODAY

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

_index_template = 'crab-tape-recall-rules-ekong'
client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())
idx = client.get_or_create_index(timestamp=time.time(), index_template=_index_template, index_mod="M")
no_of_fail_saved = client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)

print("==================================== RUCIO : Rules, RSEs, Dataset ===================================="
      , "FINISHED : "
      , len(docs), "ROWS ARE SENT"
      , no_of_fail_saved, "ROWS ARE FAILED"
      , "==================================== RUCIO : Rules, RSEs, Dataset ====================================", sep='\n')
