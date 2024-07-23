LAYER=$1
MID_LAYER=$2

CONTINUE="False"

if [[ "X$LAYER" == "XBASE" ]]; then
    file_name="/tmp/pkgs-info-base"
elif [[ "X$LAYER" == "XIMAGE" ]]; then
    file_name="/tmp/package-image.tmp"
    CONTINUE="True"
else
    exit 1
fi

rpm -qa --queryformat '%{NAME} %{VERSION}-%{RELEASE}\n' | sort >> $file_name

if [[ "X$CONTINUE" == "XTrue" ]]; then
    grep -vf /tmp/pkgs-info-base /tmp/package-image.tmp >> /tmp/pkgs-info
    rm -f /tmp/package-image.tmp
fi

if [[ "X$MID_LAYER" == "XTrue" ]]; then
    rm -f /tmp/pkgs-info-base
fi
