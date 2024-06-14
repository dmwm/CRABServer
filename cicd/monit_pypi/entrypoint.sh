#!/bin/sh

# ensure container has needed mounts
check_link(){
# function checks if symbolic links required to start service exists and if they are not broken

  if [ -L $1 ] ; then
    if [ -e $1 ] ; then
       return 0
    else
       unlink $1
       return 1
    fi
  else
    return 1
  fi
}

declare -A links=( ["/data/srv/TaskManager/logs"]="/data/hostdisk/${SERVICE}/logs")

for name in "${!links[@]}";
do
  check_link "${name}" || ln -s "${links[$name]}" "$name"
done

exec "$@"
