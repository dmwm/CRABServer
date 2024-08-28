#! /bin/bash
# Container main process.

set -euo pipefail

# run monitoring script
if [ -f /data/monitor.sh ]; then
    /data/monitor.sh &
fi

# create named pipe to pipe log to stdout
mkfifo /data/srv/state/crabserver/crabserver-fifo
# Run cat on named pipe to prevent crabserver deadlock because no reader attach
# to pipe. It is safe because only single process can read from pipe at the time
cat /data/srv/state/crabserver/crabserver-fifo &

# start the service
# manage.py required service name, anything is fine because we never use it.
./manage.py start -c -s REST

# cat fifo forever to read logs
while true;
do
    cat /data/srv/state/crabserver/crabserver-fifo
done
