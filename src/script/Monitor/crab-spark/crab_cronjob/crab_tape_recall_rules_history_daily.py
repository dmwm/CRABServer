# import pickle
from datetime import datetime, timedelta

# import click
import os
import pandas as pd
# import pprint
import time
# from dateutil.relativedelta import relativedelta
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
# import math
import osearch
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

# Query date

TODAY = str(datetime.now())[:10]
TOYEAR = TODAY[:4]
YESTERDAY = str(datetime.now()-timedelta(days=1))[:10]

# Data date

wa_date = TODAY

# Import data into database form

HDFS_RUCIO_RULES_HISTORY = f'/project/awg/cms/rucio/{wa_date}/rules_history/'

print("==============================================="
      , "RUCIO : Rules History"
      , "==============================================="
      , "File Directory:", HDFS_RUCIO_RULES_HISTORY
      , "Work Directory:", os.getcwd()
      , "==============================================="
      , "===============================================", sep='\n')

rucio_rules_history = spark.read.format('avro').load(HDFS_RUCIO_RULES_HISTORY).withColumn('ID', lower(_hex(col('ID'))))

# Query data in daily

rucio_rules_history = rucio_rules_history.select("ID", "NAME", "STATE", "EXPIRES_AT", "UPDATED_AT", "CREATED_AT", "ACCOUNT").filter(f"""ACCOUNT IN ('crab_tape_recall')""").cache()
rucio_rules_history.createOrReplaceTempView("rules_history")

query = query = f"""\
WITH filter_t AS (
SELECT ID, NAME, STATE, EXPIRES_AT, UPDATED_AT, CREATED_AT
FROM rules_history 
WHERE 1=1
AND CREATED_AT >= unix_timestamp("{TOYEAR}-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000
),
rn_t AS (
SELECT ID, NAME, STATE, EXPIRES_AT, UPDATED_AT, CREATED_AT,
row_number() over(partition by ID order by UPDATED_AT desc) as rn
FROM filter_t
),
calc_days_t AS (
SELECT ID, NAME, STATE, EXPIRES_AT, UPDATED_AT, CREATED_AT,
   CASE 
      WHEN STATE = 'O' THEN ceil((UPDATED_AT-CREATED_AT)/86400000)  
      WHEN STATE != 'O' AND EXPIRES_AT < unix_timestamp("{wa_date} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000 THEN ceil((EXPIRES_AT-CREATED_AT)/86400000)
      ELSE 0
   END AS DAYS
FROM rn_t
WHERE rn = 1
)
SELECT * 
FROM calc_days_t
WHERE 1=1
AND EXPIRES_AT >= unix_timestamp("{YESTERDAY} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000
AND EXPIRES_AT < unix_timestamp("{TODAY} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000 
"""

tmpdf = spark.sql(query)
tmpdf.show()

# Convert database to dictionary

docs = tmpdf.toPandas().to_dict('records')

# Define type of each schema

def get_index_schema():
    return {
        "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
        "mappings": {
            "properties": {
                "timestamp": {"format": "epoch_second", "type": "date"},
                "ID": {"ignore_above": 1024, "type": "keyword"},
                "NAME": {"ignore_above": 2048, "type": "keyword"},
                "STATE": {"ignore_above": 1024, "type": "keyword"},
                "EXPIRES_AT": {"format": "epoch_millis", "type": "date"},
                "UPDATED_AT": {"format": "epoch_millis", "type": "date"},
                "CREATED_AT": {"format": "epoch_millis", "type": "date"},
                "DAYS": {"type": "long"},
            }
        }
    }

# Send data to Opensearch

_index_template = 'crab-tape-recall-daily-ekong'
client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())
idx = client.get_or_create_index(timestamp=time.time(), index_template=_index_template, index_mod="M")
no_of_fail_saved = client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)

print("=================================== RUCIO : Rules History ====================================="
      , "FINISHED : "
      , len(docs), "ROWS ARE SENT"
      , no_of_fail_saved, "ROWS ARE FAILED"
      , "=================================== RUCIO : Rules History =====================================", sep='\n')
