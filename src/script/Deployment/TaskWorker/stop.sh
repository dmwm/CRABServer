#!/bin/bash
# gracefully stop TaskWorker by issueing SIGTERM to the Master
TaskMasterPid=`ps exfww|grep MasterWorker|grep -v grep|head -1|awk '{print $1}'`
kill $TaskMasterPid
