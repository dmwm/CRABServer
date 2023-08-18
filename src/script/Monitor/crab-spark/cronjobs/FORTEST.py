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

from pyspark.sql.types import (
    LongType,
)

import numpy as np
import json
import osearch
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

# Query date

TODAY = str(datetime.now())[:10]
YESTERDAY = str(datetime.now()-timedelta(days=1))[:10]

# Data date

wa_date = TODAY

# Import data into database form

HDFS_CRAB_part = f'/project/awg/cms/crab/tasks/{wa_date}/'
print("===============================================", "File Directory:", HDFS_CRAB_part, "Work Directory:", os.getcwd(), "===============================================", sep='\n')

crab_part = spark.read.format('avro').load(HDFS_CRAB_part)
df = crab_part.select("TM_TASKNAME","TM_START_TIME","TM_TASK_STATUS","TM_SPLIT_ALGO","TM_USERNAME")
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

print(docs[:5])