#!/bin/sh

PYTHONPATH=~/projects/WMCore/src/python:$PYTHONPATH

exec python2.6 src/python/CRABInterface/DagmanDataWorkflow.py

