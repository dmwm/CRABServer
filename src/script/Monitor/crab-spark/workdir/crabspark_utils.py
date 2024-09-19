from datetime import timedelta

def get_candidate_files(start_date, end_date, spark, base, day_delta=1):
    """
    Returns a list of hdfs folders that can contain data for the given dates.
    Copy from CMSMONIT CMSSpark:
    https://github.com/dmwm/CMSSpark/blob/b8efa0ac5cb57b617ee8d1ea9bb26d53fb0443b0/src/python/CMSSpark/spark_utils.py#L768
    """
    st_date = start_date - timedelta(days=day_delta)
    ed_date = end_date + timedelta(days=day_delta)
    days = (ed_date - st_date).days

    sc = spark.sparkContext
    # The candidate files are the folders to the specific dates,
    # but if we are looking at recent days the compaction procedure could
    # have not run yet, so we will consider also the .tmp folders.
    candidate_files = [
        f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}{{,.tmp}}"
        for i in range(0, days)
    ]
    fsystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    uri = sc._gateway.jvm.java.net.URI
    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = fsystem.get(uri("hdfs:///"), sc._jsc.hadoopConfiguration())
    candidate_files = [url for url in candidate_files if fs.globStatus(path(url))]
    return candidate_files
