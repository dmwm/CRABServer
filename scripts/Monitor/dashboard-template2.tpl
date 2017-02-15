            ]
          },
          "title": "ALARMS"
        },
        {
          "error": false,
          "span": 3,
          "editable": true,
          "type": "text",
          "loadingEditor": false,
          "mode": "html",
          "content": "To see which hosts are showing gLExec errors at a site click <br/> <a href=\"https://meter.cern.ch/public/_plugin/kibana/#/dashboard/temp/CMS::CRABglexec\" target=\"_blank\">here</a>",
          "style": {},
          "title": ".."
        }
      ],
      "notice": false
    },
    {
      "title": "Tasks Overview",
      "height": "200px",
      "editable": true,
      "collapse": false,
      "collapsable": true,
      "panels": [
        {
          "span": 6,
          "editable": true,
          "type": "multifieldhistogram",
          "loadingEditor": false,
          "values": [
            {
              "mode": "mean",
              "value_field": "@message.nv_value",
              "scale": 1,
              "zerofill": true,
              "derivative": false,
              "queries": [
                5
              ],
              "color": null,
              "alias": "DAGs (Run)"
            },
            {
              "mode": "mean",
              "value_field": "@message.nv_value",
              "scale": 1,
              "zerofill": true,
              "derivative": false,
              "queries": [
                6
              ],
              "color": null,
              "alias": "DAGs (Idle)"
            },
            {
              "mode": "mean",
              "value_field": "@message.nv_value",
              "scale": 1, 
              "zerofill": true,
              "derivative": false,
              "queries": [
                7
              ],
              "color": null,
              "alias": "TaskProcesses (Run)"
            }
          ],
          "time_field": "@timestamp",
          "x-axis": true,
          "y-axis": true,
          "y_format": "none",
          "grid": {
            "max": null,
            "min": 0
          },
          "annotate": {
            "enable": false,
            "query": "*",
            "size": 20,
            "field": "_type",
            "sort": [
              "_score",
              "desc"
            ]
          },
          "auto_int": true,
          "resolution": 100,
          "interval": "10m",
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
          "lines": true,
          "fill": 0,
          "linewidth": 3,
          "points": false,
          "pointradius": 5,
          "stack": false,
          "spyable": true,
          "zoomlinks": true,
          "options": true,
          "legend": true,
          "show_query": true,
          "interactive": true,
          "legend_counts": true,
          "timezone": "browser",
          "percentage": false,
          "tooltip": {
            "value_type": "cumulative",
            "query_as_alias": true
          },
          "title": "CRAB TASKS IN ALL SCHEDDs"
        }
      ],
      "notice": false
    },
    {
      "notice": false,
      "collapsable": true,
      "collapse": false,
      "title": "SCHEDDS VIEWS",
      "editable": true,
      "height": "250px",
      "panels": [
