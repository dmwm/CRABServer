from __future__ import print_function

import os
import argparse
import subprocess


def parse_options():
    """ Parse the options of the script. 
    """
    parser = argparse.ArgumentParser(
        description='Print information after analyzing the cmsweb logfiles')
    parser.add_argument(
        'logfiles', metavar='logs', nargs='+', type=str,
        help='The logfiles you want to analyze')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    """ The script takes a series of filenames as input. Those filenames are
        the logfiles produced by the CRAB cmsweb backend. Lines are in this format:

[16/Feb/2017:01:00:01] vocms0136.cern.ch 188.184.85.85 "PUT /crabserver/prod/filemetadata HTTP/1.1" 200 OK [data: 3031 in 50 out 37654 us ] [auth: OK "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=aspiezia/CN=676213/CN=Aniello Spiezia" "" ] [ref: "" "CRABClient/0.0.0" ]

        Bash magic characters are accepted, so you can pass something like:
             /build/srv-logs/vocms*/crabserver/crabserver-20170215.log
        to analyze all the logs of a particular day.

        The output is a series of metrics for each one of the file that are divided
        per query type (e.g.: status long vs ASO/Oracle queries etc)
        Currently supported metrics are the number of queries each user did,
        and the average lenght of each query.
    """
    os.environ["LC_NUMERIC"] = "en_US"
    args = parse_options()

    filenames = args.logfiles

    queries = [
        ('Status', r'GET /crabserver/prod/workflow\?verbose=0'),
        ('Status long', r'GET /crabserver/prod/workflow\?verbose=1'),
        ('Status idle', r'GET /crabserver/prod/workflow\?verbose=2'),
        ('Other Status', r'GET /crabserver/prod/workflow\?workflow'),
        ('Submit', r'PUT /crabserver/prod/workflow'),
        ('Filemetadata GET', r'GET /crabserver/prod/filemetadata\?taskname'),
        ('Filemetadata PUT', r'PUT /crabserver/prod/filemetadata'),
        ('Resubmit', 'POST /crabserver/prod/workflow'),
        ('Task resource', '/crabserver/prod/task'),
        ('Info resource', r'GET /crabserver/prod/info'),
        ('TaskWorker', r'(GET|POST|PUT) /crabserver/prod/workflowdb'),
        ('ASO/Oracle', r'(GET|POST|PUT) (/crabserver/prod/filetransfers|/crabserver/prod/fileusertransfers)'),
    ]

    filtertypes = [
        ("Top users", r"egrep '%s' %s | cut -d\" -f4 | sort | uniq -c | sort -n | tail -n 2"),
        ("Query total time, count, and avg (us)", "egrep '%s' %s | awk '{total += $14; count++} END {print total \" \" count; printf \"%%f\", total/count}'"),
    ]

    for file_ in filenames:
        print("Analyzing file %s." % file_)
        for ftname, ftcmd in filtertypes:
            print('\t' + ftname + ':')
            for qname, qgrep in queries:
                cmd = ftcmd % (qgrep, file_)
                print(cmd)
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                out = p.communicate()[0]
                print('\t\t' + qname)
                print('\t\t\t' + '\n\t\t\t'.join(out.split('\n')))

#    Small piece of code to check if all the queries been catched. Commented because data2.txt is hardcoded
#    This can be turned into an option in the future.
#    cmd = "|".join(map(lambda x: x[1], queries))
#    cmd = "egrep '(GET|PUT|POST|DELETE)' data2.txt | grep -v RESTSQL | egrep -v '" + cmd + "'"
#    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#    out = p.communicate()[0]
#    print (out)
