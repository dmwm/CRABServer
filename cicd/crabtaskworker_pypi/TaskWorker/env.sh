##H Usage example: ./start.sh -c | -g [-d]"
##H     -c start current crabserver instance"
##H     -g start crabserver instance from GitHub repo"
##H     -d start crabserver in debug mode. Option can be combined with -c or -g"

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

helpFunction() {
    grep "^##H" "${0}" | sed -r "s/##H(| )//g"
}


DEBUG=''
MODE=''
while getopts ":dDcCgGhH" o; do
    case "${o}" in
        h|H) helpFunction ;;
        g|G) MODE="fromGH" ;;
        c|C) MODE="current" ;;
        d|D) DEBUG=true ;;
        * ) echo "Unimplemented option: -$OPTARG"; helpFunction; exit 1 ;;
    esac
done
shift $((OPTIND-1))

if ! [[ -v MODE ]]; then
  echo "Please set how you want to start crabserver (add -c or -g option)." && helpFunction
fi
markmodify_path=/data/srv/current/data_files_modified
case $MODE in
    current)
        # current mode: run current instance
        # Refuse to start when data_files directry is modified.
        if [[ -f ${markmodify_path} ]]; then
            echo "data_files directory has been modifed."
            echo "Refuse to start with -c option."
            exit 1
        fi
        PYTHONPATH=/data/srv/current/lib/python/site-packages:${PYTHONPATH:-}
        ;;
    fromGH)
        # private mode: run private instance from GH
        PYTHONPATH=/data/repos/CRABServer/src/python:/data/repos/WMCore/src/python:${PYTHONPATH:-}
        # update runtime (create TaskManagerRun.tar.gz from source)
        touch ${markmodify_path}
        ./updateDatafiles.sh
        ;;
    *) echo "Unimplemented mode: $MODE"; exit 1 ;;
esac

# Export PYTHONPATH and DEBUG to ./manage.sh
export PYTHONPATH
export DEBUG
