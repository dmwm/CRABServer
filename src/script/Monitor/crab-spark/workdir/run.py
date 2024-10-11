#!/usr/bin/env python3

"""
This file convert the spark notebook into python file and run spark-submit
Require shell to source "bootstrap.sh" to bootstrap the cmd and pylib need by
this script.

It process the python's argparse and pass the argument to spark script via
environment variable.

For examples:
- To extract data from the whole September 2024
    ./run.py --secretpath secret.txt --start 2024-09-01 --end 2024-10-01 crab_taskdb.ipynb

- To extract data from n days ago (in case you need to wait until data settle)
  For example, today is 2024-10-01 but you want to process data on 2024-09-30
    ./run.py --secretpath secret.txt --ndaysago 2 crab_condor.ipynb

- To push result docs to production index (otherwise, index will prefix with `crab-test`)
   ./run.py --secretpath secret.txt --today --prod crab_taskdb.ipynb

- To run in crontab daily, use "run_spark.sh" to prepare a new shell and source bootstrap.sh
    ./run_spark.sh ./run.py --secretpath secret.txt --today --prod crab_taskdb.ipynb

- To check env that will pass to spark script
     ./run.py --secretpath secret.txt --today --dryrun crab_taskdb.ipynb
"""
import argparse
import os
import subprocess
import pathlib
from pprint import pprint
from datetime import datetime, timedelta, timezone

def valid_date(s):
    """
    check if date formate is correct and return the arg `s`.
    The function serve as `type` of argument in argparse.

    >>> valid_date('2024-01-01')
    valid_date('2024-01-01')

    :param s: date in format YYYY-mm-ddd
    :type s: str

    :return: s argument
    :rtype: str
    """
    try:
        datetime.strptime(s, '%Y-%m-%d')
        return s
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"not a valid date: {s!r}") from e

parser = argparse.ArgumentParser(description='Converting spark ipynb and run spark-submit')
parser.add_argument('path', help='path of script (.ipynb)')
parser.add_argument('--start', type=valid_date, dest='start_date', help='Start date of interest (YYY-mm-dd)')
parser.add_argument('--end', type=valid_date, dest='end_date', help='End date of interest (YYY-mm-dd).')
parser.add_argument('--ndaysago', type=int, default=-1, help='set start date to n-1 days ago, and end date to n days ago')
parser.add_argument('--today', action='store_true', help='shortcut --ndaysago 0')
parser.add_argument('--prod', action='store_true', help='set opensearch index prefix to prod "crab-<index_name>)". Default is "crab-test-<index_name>"')
parser.add_argument('--secretpath', help='secret file path')
parser.add_argument('--dryrun', action='store_true', help='print env that will pass to spark script')
args = parser.parse_args()

sparkjob_env = {}
if args.today:
    args.ndaysago = 0
if args.ndaysago >= 0:
    day = datetime.now().replace(tzinfo=timezone.utc)
    ed = args.ndaysago
    sd = args.ndaysago + 1 # start date is "yesterday" of n days ago
    sparkjob_env['START_DATE'] = (day-timedelta(days=sd)).strftime("%Y-%m-%d")
    sparkjob_env['END_DATE'] = (day-timedelta(days=ed)).strftime("%Y-%m-%d")
if args.start_date and args.end_date:
    sparkjob_env['START_DATE'] = args.start_date
    sparkjob_env['END_DATE'] = args.end_date
if 'START_DATE' not in sparkjob_env and 'END_DATE' not in sparkjob_env:
    raise Exception("Need --today or --ndaysago or --start/--end.")
if args.secretpath:
    sparkjob_env['OPENSEARCH_SECRET_PATH'] = args.secretpath
if args.prod:
    sparkjob_env['PROD'] = 't'
else:
    sparkjob_env['PROD'] = 'f'

runenv = os.environ.copy()
runenv.update(sparkjob_env)

# convert from nootebook to py
path = pathlib.Path(args.path)
pathpy = path.with_suffix('.py')
cmd = f"jupyter nbconvert --to python {path}"
print(f'Running: {cmd}')
if not args.dryrun:
    subprocess.run(cmd, shell=True, timeout=3600, check=True)

# spark-submit
cmd = f'spark-submit --master yarn --packages org.apache.spark:spark-avro_2.12:3.5.0 {pathpy}'
print(f'Running: {cmd}')
print('With env: ')
pprint(sparkjob_env)
if not args.dryrun:
    subprocess.run(cmd, shell=True, timeout=3600, check=True, env=runenv)
