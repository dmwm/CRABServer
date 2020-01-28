#!/bin/bash
# remove ALL old logs (mostly useful on test machines)
rm -rf /data/srv/TaskManager/logs/tasks/*
rm -rf /data/srv/TaskManager/logs/processes/*
rm -f /data/srv/TaskManager/logs/*txt*
rm -f /data/srv/TaskManager/logs/recurring.log
rm /data/srv/TaskManager/nohup.out
