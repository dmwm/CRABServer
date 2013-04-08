#!/bin/sh

# This file helps to bootstrap the CRAB3 server environment
# so you can run the DataWorkflow step manually

export PYTHONPATH=~/projects/CAFUtilities/src/python:~/projects/WMCore/src/python:~/projects/CRABServer/src/python:$PYTHONPATH

exec python2.6 src/python/CRABInterface/DagmanDataWorkflow.py

