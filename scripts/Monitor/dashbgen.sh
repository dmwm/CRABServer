#!/bin/bash
# generate n visible distinct colors
# just left this code here for reference
# will use a fixed color pallete
#COLORS=($(cat << EOF | python -
#def get_spaced_colors(n):
#    max_value = 16581375
#    # distance between colors
#    interval = int(max_value / n) 
#    colors = [hex(I)[2:].zfill(6) for I in range(0, max_value, interval)]
#    return [ "#%s%s%s"%(i[:2], i[2:4], i[4:]) for i in colors ]
#
#for color in get_spaced_colors(11):
#  print "%s" % color
#EOF
#))

# fixed color pallete generated online at http://phrogz.net/css/distinct-colors.html using 
# following settings:
# HUE 
#   default values
# SATURATION 
#   start: 100% end: 50% step: 50%
# VALUES
#   default values
#
# click on generate and then go to results
# and choose an interleave of 13, copy all HEX values, paste on your favorite text editor,
# replace all commas for nothing and all "#" for "\#"(without quotes)
#
#
# usage: 
# ./dashbgen.sh > CRAB.json
# cp CRAB.json  /afs/cern.ch/cms/monitoring/CMS/CRAB.json 
#
#
COLORS=(\#b20000 \#732e00 \#f2c200 \#475900 \#008c25 \#005953 \#6cb5d9 \#8091ff \#3e2d59 \#da79f2 \#cc669c \#e5003d \#d96c6c \#cc6d00 \#59502d \#0e3300 \#73e691 \#003d4d \#00388c \#3300bf \#3d0073 \#99008f \#99003d \#4c0014 \#402820 \#a68a53 \#99bf00 \#20f200 \#00f2e2 \#00a2f2 \#405980 \#0e0033 \#aa00ff \#f200a2 \#592d3e)

SCHEDDS=(crab3@vocms0106.cern.ch crab3@vocms0121.cern.ch crab3@vocms0122.cern.ch  crab3@vocms0107.cern.ch crab3@vocms0137.cern.ch crab3@vocms0144.cern.ch crab3@vocms0155.cern.ch crab3@vocms0119.cern.ch crab3@vocms0120.cern.ch crab3@vocms0194.cern.ch crab3@vocms0195.cern.ch crab3@vocms0196.cern.ch  crab3@vocms0197.cern.ch crab3@vocms0198.cern.ch crab3@vocms0199.cern.ch)

SCHEDD_SNAME=(vocms0106 vocms0121 vocms0122 vocms0107 vocms0137 vocms0144 vocms0155 vocms0119 vocms0120 vocms0194 vocms0195 vocms0196  vocms0197 vocms0198 vocms0199)

# python function to populate SCHEDDS variable with all
# schedds on crab external rest configuration
# --- NOT USED AT THIS MOMENT ---
: 'SCHEDDS=($(cat << EOF | python -
from urllib import urlopen
import json
url="https://gitlab.cern.ch/crab3/CRAB3ServerConfig/raw/master/cmsweb-rest-config.json"
response = urlopen(url)
data = json.loads(response.read())
schedds=[ x for x in data["cmsweb-prod"]['backend-urls']['htcondorSchedds'].keys() ]
names=""
for sc in schedds:
  names+=sc+' '
print names
EOF
))

SCHEDD_SNAME=($(cat << EOF | python -
from urllib import urlopen
import sys
import json
url="https://gitlab.cern.ch/crab3/CRAB3ServerConfig/raw/master/cmsweb-rest-config.json"
response = urlopen(url)
data = json.loads(response.read())
schedds = data["cmsweb-prod"]['backend-urls']['htcondorSchedds'].keys()
shortNames = [ x.split('@')[1].split('.')[0] for x in schedds]
names=""
for sc in shortNames:
  names+=sc+" "
print names
EOF
))'


function panelStarts(){
cat << EOF
        {
          "show_query": false,
          "interval": "5m",
          "zoomlinks": false,
          "annotate": {
            "sort": [
              "_score",
              "desc"
            ],
            "query": "*",
            "enable": false,
            "size": 20,
            "field": "_type"
          },
          "intervals": [
            "auto",
            "1s",
            "1m",
            "5m",
            "10m",
            "30m",
            "1h",
            "3h",
            "12h",
            "1d",
            "1w",
            "1y"
          ],
          "timezone": "browser",
          "spyable": true,
          "linewidth": 3,
          "percentage": false,
          "fill": 0,
          "span": 4,
          "title": "${1}",
          "tooltip": {
            "value_type": "individual",
            "query_as_alias": true
          },
          "legend": true,
          "loadingEditor": false,
          "auto_int": false,
          "type": "multifieldhistogram",
          "x-axis": true,
          "editable": true,
          "grid": {
            "max": null,
            "min": null
          },
          "stack": false,
          "legend_counts": false,
          "time_field": "@timestamp",
          "y-axis": true,
          "lines": true,
          "y_format": "short",
          "points": false,
          "values": [
EOF
}

function panelEnds(){
cat << EOF
          ],
          "pointradius": 5,
          "resolution": 100,
          "options": false,
          "interactive": true
        }
EOF
}

# dasboard beginning
cat dashboard-template1.tpl

# schedd active alarms
SCHEDD_QID=400

for schedd in ${SCHEDD_SNAME[@]};do
  re=.*vocms.*
  if [[ ${schedd} =~ $re ]]; then
      SCHEDD_QID=$((SCHEDD_QID+1))
      echo -ne "\t       $SCHEDD_QID"
     if [ "$schedd" != "${SCHEDD_SNAME[${#SCHEDD_SNAME[@]}-1]}" ];then
        echo -ne ',\n'
     else
        echo 
     fi
  fi
done

cat dashboard-template2.tpl

# Panel with number of shadows per Schedd
panelStarts "[3] Number of Running Jobs per Schedd"
# all schedd series for number of shadows
SCHEDD_QID=100
for schedd in ${SCHEDD_SNAME[@]};do
  SCHEDD_QID=$((SCHEDD_QID+1))
  # Number of DAGs per schedd
  echo -ne `cat << EOF
            \t{\n
            \t  "value_field": "@message.nv_value",\n
            \t  "scale": 1,\n
            \t  "scaleSeconds": true,\n
            \t  "color": null,\n
            \t  "zerofill": false,\n
            \t  "alias": "${schedd}",\n
            \t  "mode": "mean",\n
            \t  "queries": [\n
            \t    ${SCHEDD_QID}\n
            \t  ],\n
            \t  "derivative": false\n
            \t}
EOF`

   if [ "$schedd" != "${SCHEDD_SNAME[${#SCHEDD_SNAME[@]}-1]}" ];then
     echo -ne ',\n'
   else
     echo 
   fi
done
panelEnds
echo ','

# Panel with idle jobs per schedds
panelStarts "[4] Number of Idle Jobs per Schedd"
SCHEDD_QID=200
for schedd in ${SCHEDD_SNAME[@]};do
  SCHEDD_QID=$((SCHEDD_QID+1))
  # Number of DAGs per schedd
  echo -ne `cat << EOF
            \t{\n
            \t  "value_field": "@message.nv_value",\n
            \t  "scale": 1,\n
            \t  "scaleSeconds": true,\n
            \t  "color": null,\n
            \t  "zerofill": false,\n
            \t  "alias": "${schedd}",\n
            \t  "mode": "mean",\n
            \t  "queries": [\n
            \t    ${SCHEDD_QID}\n
            \t  ],\n
            \t  "derivative": false\n
            \t}
EOF`

   if [ "$schedd" != "${SCHEDD_SNAME[${#SCHEDD_SNAME[@]}-1]}" ];then
     echo -ne ',\n'
   else
     echo 
   fi
done
panelEnds
echo ','

# Panel with DAGs per schedd
panelStarts "[5] Running DAGMAN's per schedd"
SCHEDD_QID=300
for schedd in ${SCHEDD_SNAME[@]};do
  SCHEDD_QID=$((SCHEDD_QID+1))
  # Number of DAGs per schedd
  echo -ne `cat << EOF
            \t{\n
            \t  "value_field": "@message.nv_value",\n
            \t  "scale": 1,\n
            \t  "scaleSeconds": true,\n
            \t  "color": null,\n
            \t  "zerofill": false,\n
            \t  "alias": "${schedd}",\n
            \t  "mode": "mean",\n
            \t  "queries": [\n
            \t    ${SCHEDD_QID}\n
            \t  ],\n
            \t  "derivative": false\n
            \t}
EOF`

   if [ "$schedd" != "${SCHEDD_SNAME[${#SCHEDD_SNAME[@]}-1]}" ];then
     echo -ne ',\n'
   else
     echo 
   fi
done
panelEnds

# links for schedd host metrics
cat << EOF
      ]
    },
    {
      "title": "SCHEDDS HOST METRICS/NOTIFICATIONS",
      "height": "100px",
      "editable": true,
      "collapse": true,
      "collapsable": true,
      "panels": [
        {
          "error": false,
          "span": 12,
          "editable": true,
          "type": "text",
          "loadingEditor": false,
          "mode": "html",
EOF

echo -ne '          "content": "'
echo -ne '<table width=100%>'
echo -ne '<tr>'
echo -ne '<th width=50% align=center>SCHEDD HOST METRICS BY HOST</th>'
echo -ne '<th width=50% align=center>SCHEDD ACTIVE ALARMS AND NOTIFICATIONS BY HOST</th>'
echo -ne '</tr>'
echo -ne '<tr><td align=center>'
for schedd in ${SCHEDD_SNAME[@]};do
  re=.*vocms.*
  if [[ ${schedd} =~ $re ]]; then
    SCHEDD_QID=$((SCHEDD_QID+1))
    echo -ne '<a href=\"https://meter.cern.ch/public/_plugin/kibana/#/dashboard/elasticsearch/Metrics:%20Host?query='${schedd}'\" target=\"_blank\">'${schedd}'</a> '
  fi
done
echo -ne '</td><td align=center>'

# links for schedds alarms and notifications
for schedd in ${SCHEDD_SNAME[@]};do
  re=.*vocms.*
  if [[ ${schedd} =~ $re ]]; then
    SCHEDD_QID=$((SCHEDD_QID+1))
    echo -ne '<a href=\"https://meter.cern.ch/public/_plugin/kibana/#/dashboard/temp/CMS::Notifications?query='${schedd}'\" target=\"_blank\">'${schedd}'</a> '
  fi
done
echo -ne '</td></tr></table>'
echo -ne '",\n'

cat dashboard-fixed-queries.tpl
################################################# Queries Section 
# queries for number of shadows running
SCHEDD_QID=100
SCHEDD_ID=0
COLOR_ID=0
for schedd in ${SCHEDD_SNAME[@]};do
  SCHEDD_QID=$((SCHEDD_QID+1))
  # Number of Shadows per schedd
  echo -ne `cat << EOF
        \t"$SCHEDD_QID": {\n
        \t  "enable": true,\n
        \t  "pin": false,\n
        \t  "color": "${COLORS[$COLOR_ID]}",\n
        \t  "alias": "${schedd}Shadows",\n
        \t  "query": "vocms0118 AND @message.nv_name:\"number_of_shadows_process_for_${SCHEDDS[${SCHEDD_ID}]}\"",\n
        \t  "type": "lucene",\n
        \t  "id": $SCHEDD_QID\n
        \t},\n
EOF`
  SCHEDD_ID=$((SCHEDD_ID+1))
  COLOR_ID=$((COLOR_ID+1))
done


# queries for idle jobs per schedd
SCHEDD_QID=200
SCHEDD_ID=0
COLOR_ID=0
for schedd in ${SCHEDDS[@]};do
  SCHEDD_QID=$((SCHEDD_QID+1))
  # Number of shadows per schedd
  echo -ne `cat << EOF
        \t"$SCHEDD_QID": {\n
         \t "enable": true,\n
         \t "pin": false,\n
         \t "color": "${COLORS[$COLOR_ID]}",\n
         \t "alias": "idleJobsAt${schedd}",\n
         \t "query": "vocms0118 AND @message.nv_name:\"number_of_idle_jobs_for_at_${SCHEDDS[${SCHEDD_ID}]}\"",\n
         \t "type": "lucene",\n
         \t "id": $SCHEDD_QID\n
        \t},\n
EOF`
  SCHEDD_ID=$((SCHEDD_ID+1))
  COLOR_ID=$((COLOR_ID+1))
done

# queries from running DAGs per schedd
SCHEDD_QID=300
SCHEDD_ID=0
COLOR_ID=0
for schedd in ${SCHEDDS[@]};do
  SCHEDD_QID=$((SCHEDD_QID+1))
  # Number of shadows per schedd
  echo -ne `cat << EOF
        \t"$SCHEDD_QID": {\n
        \t "enable": true,\n
        \t "pin": false,\n
        \t "color": "${COLORS[$COLOR_ID]}",\n
        \t "alias": "${schedd}SchedulersRunning",\n
        \t "query": "vocms0118 AND @message.nv_name:\"number_of_schedulers_jobs_running_for_${SCHEDDS[${SCHEDD_ID}]}\"",\n
        \t "type": "lucene",\n
        \t "id": $SCHEDD_QID\n
        \t},\n
EOF`
  SCHEDD_ID=$((SCHEDD_ID+1))
  COLOR_ID=$((COLOR_ID+1))
done


# queries for schedd alarms
SCHEDD_QID=400
SCHEDD_ID=0
COLOR_ID=0
ALARM_COUNT=0
for schedd in ${SCHEDD_SNAME[@]};do
  re=.*vocms.*
  if [[ ${schedd} =~ $re ]]; then
    SCHEDD_QID=$((SCHEDD_QID+1))
#        \t  "query": "${schedd} AND body.metadata.state:active",\n
    echo -ne `cat << EOF 
        \t"${SCHEDD_QID}": {\n
        \t  "enable": true,\n
        \t  "pin": true,\n
        \t  "color": "${COLORS[$COLOR_ID]}",\n
        \t  "alias": "${schedd}",\n
        \t  "query": "@fields.entity:${schedd} AND @fields.metric_name:exception.*",\n
        \t  "type": "lucene",\n
        \t  "id": ${SCHEDD_QID}\n
        \t}
EOF`

    if [ "$schedd" != "${SCHEDD_SNAME[${#SCHEDD_SNAME[@]}-1]}" ];then
      echo -ne ',\n'
    else
      echo -ne "\n      },\n"
    fi
    SCHEDD_ID=$((SCHEDD_ID+1))
    COLOR_ID=$((COLOR_ID+1))
    ALARM_COUNT=$((ALARM_COUNT+1))
  fi
done

#### query id list
cat << EOF
      "ids": [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
	7,
	15,
        16,
        17,
        18,
        19,
        35,
        42,
        43,
        44,
        53,
        54,
        55,
        56,
EOF

# Write all others queries
for q in `seq 100 $((100 + ${#SCHEDD_SNAME[@]}))`;do
  echo "        $q,"
done

for q in `seq 200 $((200 + ${#SCHEDD_SNAME[@]}))`;do
  echo "        $q,"
done

for q in `seq 300 $((300 + ${#SCHEDD_SNAME[@]}))`;do
  echo "        $q,"
done

for q in `seq 400 $((400 + $ALARM_COUNT - 1))`;do
  echo -ne "        $q,\n"
done

q=$((q+1))
echo -ne "        $q\n"


cat << EOF
      ],
      "idQueue": [
        0,
        1,
        3,
        4,
        5
      ]
    }
  },
  "panel_hints": true
}
EOF

