import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import time
import numpy as np
import pandas as pd

from datetime import datetime, date, timedelta

import osearch

import argparse
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-s", "--start-date", default=None,
  help="process data starting from this day, inclusive (YYYY-MM-DD)",)
parser.add_argument("-e", "--end-date", default=None,
  help="process data until this day, not included (YYYY-MM-DD)",)
args = parser.parse_args()
print(f"timerange: [{args.start_date} {args.end_date})" )


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
from pyspark.sql.types import (
    StructType,
    LongType,
    StringType,
    StructField,
    DoubleType,
    IntegerType,
)

from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

# CRAB table date

# condor data and query date
# if args.end_date:
#     end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
# else:
#     end_date = datetime.now()
#     end_date = end_date.replace(minute=0, hour=0, second=0, microsecond=0)
# 
# if args.start_date:
#     start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
# else:
#     start_date = end_date - timedelta(days=1)

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

# Import condor data

def process_single_day(day):

    start_date = day
    end_date = day + timedelta(days=1)
    print(f"START PROCESSING: from {start_date} to {end_date}")

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
    
        sc = spark.sparkContext
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        URI = sc._gateway.jvm.java.net.URI
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        fs = FileSystem.get(URI("hdfs:///"), sc._jsc.hadoopConfiguration())
        candidate_files = [
            f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}"
            for i in range(0, days)
        ]    
        candidate_files = [url for url in candidate_files if fs.globStatus(Path(url))]
        print("No. of Compacted files:", len(candidate_files))
    
        pre_candidate_files = [
            f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}.tmp"
            for i in range(0, days)
        ]
        pre_candidate_files = [url for url in pre_candidate_files if fs.globStatus(Path(url))]
        print("No. of uncompacted files:", len(pre_candidate_files))
        
        return candidate_files + pre_candidate_files
    
    
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
            .drop_duplicates(["GlobalJobId"])
    #	.cache()
        )
    
    # Convert file type by saving and recall it again (.json too complex for spark)
    
    crab_username = os.getenv("CRAB_KRB5_USERNAME", "cmscrab")
    condor_df.write.mode('overwrite').parquet(f"/cms/users/{crab_username}/condor_vir_data" ,compression='zstd')
    condor_df = spark.read.format('parquet').load(f"/cms/users/{crab_username}/condor_vir_data")
    
    # Import CRAB data
    wa_date = day.strftime("%Y-%m-%d")    
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
    
    docs = result_df.toPandas()
    docs["CRAB_Type"] = docs.apply(lambda row: "PrivateMC" if row["CRAB_DataBlock"] == "MCFakeBlock" else "Analysis", axis=1)
    print(f"pandas dataframe size: {docs.memory_usage(deep=True).apply(lambda x: x / 1024 / 1024).sum()} MB")
    
    
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
    
    _index_template = 'crab-condor-taskdb'
    client = osearch.get_es_client("os-cms.cern.ch/es", '/data/certs/monit.d/monit_spark_crab.txt', get_index_schema())
    idx = client.get_or_create_index(timestamp=day.strftime("%s"), index_template=_index_template, index_mod="M")
    docs_rows = len(docs)
    sent = 0
    batch = 50000
    import gc
    while sent < docs_rows:
        gc.collect()
        start = sent
        end = start + batch if start + batch < docs_rows else docs_rows
        docs_tmp = docs.iloc[start:end]
        # the following line requires a lot of RAM, better do it 50_000
        # items at a time only. Keep in mind that the pandas datafram usually
        # contains about 1_000_000 rows
        docs_tmp = docs_tmp.to_dict('records')
        no_of_fail_saved = client.send(idx, docs_tmp, metadata=None, batch_size=10000, drop_nulls=False)
        sent = end
    
        print("=================================== Condor Matrix and CRAB Table =====================================",
              "FINISHED : ",
              f"start {start}, end {end}",
              len(docs_tmp), "ROWS ARE SENT",
              no_of_fail_saved, "ROWS ARE FAILED",
              "=================================== Condor Matrix and CRAB Table =====================================", 
            sep='\n')
    
    
for day in date_list:
    process_single_day(day)


