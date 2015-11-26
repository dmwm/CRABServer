#!/bin/sh

# This script is for sending an error message to the Dashboard
# in the case where the CMS python is too broken

unset LD_LIBRARY_PATH
PATH=/bin:/usr/bin
PYTHONPATH=$PWD
exec /usr/bin/python DashboardAPI.py $1

