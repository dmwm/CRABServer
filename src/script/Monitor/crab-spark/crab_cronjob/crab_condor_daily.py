import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import time

from datetime import datetime, date, timedelta
from pyspark.sql.functions import (
    col,
    lit,
    when,
    sum as _sum,
    count as _count,
    first,
    date_format,
    from_unixtime
)
import numpy as np
import pandas as pd
from pyspark.sql.types import (
    StructType,
    LongType,
    StringType,
    StructField,
    DoubleType,
    IntegerType,
)

import osearch
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

# CRAB table date

TODAY = str(datetime.now())[:10]
wa_date = TODAY

# condor data and query date

end_date = datetime.now()
end_date = end_date.replace(minute=0, hour=0, second=0, microsecond=0)
start_date = end_date-timedelta(days=1)

# Import condor data

_DEFAULT_HDFS_FOLDER = "/project/monitoring/archive/condor/raw/metric"

def _get_schema():
    return StructType(
        [
            StructField(
                "data",
                StructType(
                    [
                        StructField("RecordTime", LongType(), nullable=False),
                        StructField("CMSPrimaryDataTier", StringType(), nullable=True),
                        StructField("Status", StringType(), nullable=True),
                        StructField("WallClockHr", DoubleType(), nullable=True),
                        StructField("CoreHr", DoubleType(), nullable=True),
                        StructField("CpuTimeHr", DoubleType(), nullable=True),
                        StructField("Type", StringType(), nullable=True),
                        StructField("CRAB_DataBlock", StringType(), nullable=True),
                        StructField("GlobalJobId", StringType(), nullable=False),
                        StructField("ExitCode", LongType(), nullable=True),
                        StructField("CRAB_Workflow", StringType(), nullable=True),
                        StructField("CommittedCoreHr", StringType(), nullable=True),
                        StructField("CommittedWallClockHr", StringType(), nullable=True),
                    ]
                ),
            ),
        ]
    )

def get_candidate_files(start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER):
    st_date = start_date - timedelta(days=0)
    ed_date = end_date + timedelta(days=0)
    days = (ed_date - st_date).days
    pre_candidate_files = [
        "{base}/{day}{{,.tmp}}".format(
            base=base, day=(st_date + timedelta(days=i)).strftime("%Y/%m/%d")
        )
        for i in range(0, days)
    ]
    sc = spark.sparkContext
    candidate_files = [
        f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}"
        for i in range(0, days)
    ]
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = FileSystem.get(URI("hdfs:///"), sc._jsc.hadoopConfiguration())
    candidate_files = [url for url in candidate_files if fs.globStatus(Path(url))]
    return candidate_files
    
schema = _get_schema()

condor_df = (
        spark.read.option("basePath", _DEFAULT_HDFS_FOLDER)
        .json(
            get_candidate_files(start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER),
            schema=schema,
        ).select("data.*")
        .filter(
            f"""Status IN ('Completed')
            AND Type IN ('analysis')
            AND RecordTime >= {start_date.timestamp() * 1000}
            AND RecordTime < {end_date.timestamp() * 1000}
            """
        )
        .drop_duplicates(["GlobalJobId"]).cache()
    )

# Convert file type by saving and recall it again (.json too complex for spark)

condor_df.write.mode('overwrite').parquet("/cms/users/eatthaph/condor_vir_data" ,compression='zstd')
condor_df = spark.read.format('parquet').load('/cms/users/eatthaph/condor_vir_data')

# Import CRAB data

HDFS_CRAB_part = f'/project/awg/cms/crab/tasks/{wa_date}/'
crab_df = spark.read.format('avro').load(HDFS_CRAB_part)
crab_df = crab_df.select('TM_TASKNAME', 'TM_IGNORE_LOCALITY')

print("==============================================="
      , "Condor Matrix and CRAB Table"
      , "==============================================="
      , "File Directory:", HDFS_CRAB_part, get_candidate_files(start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER)
      , "Work Directory:", os.getcwd()
      , "==============================================="
      , "===============================================", sep='\n')

# Join condor job with CRAB data

result_df = condor_df.join(crab_df, crab_df["TM_TASKNAME"] == condor_df["CRAB_Workflow"])\
    .select('RecordTime', 'CMSPrimaryDataTier', 'WallClockHr', 'CoreHr', 'CpuTimeHr', 'ExitCode'
            , "CRAB_DataBlock", "TM_IGNORE_LOCALITY", "GlobalJobId", "CommittedCoreHr", "CommittedWallClockHr")
    
# Convert database to dictionary

docs = result_df.toPandas().to_dict('records')

# Extract 'CRAB_Type' from 'CRAB_DataBlock'

for i in range(len(docs)):
    if docs[i]['CRAB_DataBlock'] == 'MCFakeBlock':
        docs[i]['CRAB_Type'] = 'PrivateMC'
    else:
        docs[i]['CRAB_Type'] = 'Analysis'

# Define type of each schema

def get_index_schema():
    return {
        "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
        "mappings": {
            "properties": {
                "RecordTime": {"format": "epoch_millis", "type": "date"},
                "CMSPrimaryDataTier": {"ignore_above": 2048, "type": "keyword"},
                "GlobalJobId": {"ignore_above": 2048, "type": "keyword"},
                "WallClockHr": {"type": "long"},
                "CoreHr": {"type": "long"},
                "CpuTimeHr": {"type": "long"},
                "ExitCode": {"ignore_above": 2048, "type": "keyword"},
                "TM_IGNORE_LOCALITY": {"ignore_above": 2048, "type": "keyword"},
                "CRAB_Type": {"ignore_above": 2048, "type": "keyword"},
                "CRAB_DataBlock": {"ignore_above": 2048, "type": "keyword"},
                "CommittedCoreHr": {"type": "long"}, 
                "CommittedWallClockHr": {"type": "long"},
            }
        }
    }

# Send data to Opensearch

_index_template = 'crab-condor-ekong'
client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())
idx = client.get_or_create_index(timestamp=time.time(), index_template=_index_template, index_mod="M")
no_of_fail_saved = client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)

print("=================================== Condor Matrix and CRAB Table ====================================="
      , "FINISHED : "
      , len(docs), "ROWS ARE SENT"
      , no_of_fail_saved, "ROWS ARE FAILED"
      , "=================================== Condor Matrix and CRAB Table =====================================", sep='\n')

