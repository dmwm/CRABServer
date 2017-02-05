{
  "index": {
    "default": "[flume-lemon-]YYYY-MM-DD",
    "pattern": "[flume-lemon-]YYYY-MM-DD",
    "warm_fields": true,
    "interval": "day"
  },
  "style": "dark",
  "rows": [
    {
      "title": "logo",
      "height": "90px",
      "editable": true,
      "collapse": false,
      "collapsable": false,
      "panels": [
        {
          "error": false,
          "span": 12,
          "editable": true,
          "type": "text",
          "loadingEditor": false,
          "mode": "html",
          "content": "<table>\n<tr>\n<td>\n<img src=\"http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/img/crab_logo_3.png\" high='77' width='154' align='right'/>\n</td>\n<td>\n<h3>CRAB3 Operators Monitoring Dashboard</h3>\nMore info and docs about CRAB3 available at <a href=\"https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrab\" target=\"_blank\">https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrab</a>\n</td></tr></table>",
          "style": {},
          "title": "."
        }
      ],
      "notice": false
    },
    {
      "notice": false,
      "collapsable": true,
      "collapse": false,
      "title": "TASK WORKER AND GLEXEC",
      "editable": true,
      "height": "350px",
      "panels": [
        {
          "span": 4,
          "title": "..",
          "editable": true,
          "error": false,
          "loadingEditor": false,
          "panels": [
            {
              "show_query": true,
              "sizeable": false,
              "zoomlinks": false,
              "height": "200px",
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
              "linewidth": 1,
              "fill": 3,
              "loading": false,
              "span": 10,
              "title": "[1] Count of Tasks entering a given status per 2h interval",
              "tooltip": {
                "value_type": "cumulative",
                "query_as_alias": true
              },
              "stack": false,
              "removable": false,
              "percentage": false,
              "auto_int": false,
              "type": "multifieldhistogram",
              "x-axis": true,
              "editable": true,
              "grid": {
                "max": null,
                "min": 0
              },
              "legend": true,
              "legend_counts": false,
              "y-axis": true,
              "time_field": "@timestamp",
              "interval": "5m",
              "lines": true,
              "y_format": "none",
              "draggable": false,
              "points": false,
              "values": [
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": "#64B0C8",
                  "zerofill": false,
                  "alias": "Submitted",
                  "mode": "mean",
                  "queries": [
                    3
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": "#890F02",
                  "zerofill": false,
                  "alias": "Failed",
                  "mode": "mean",
                  "queries": [
                    1
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": "#58140C",
                  "zerofill": false,
                  "alias": "Killed",
                  "mode": "mean",
                  "queries": [
                    2
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": null,
                  "zerofill": false,
                  "alias": "ResubmitFailed",
                  "mode": "mean",
                  "queries": [
                    56
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": null,
                  "zerofill": false,
                  "alias": "SubmitFailed",
                  "mode": "mean",
                  "queries": [
                    55
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": null,
                  "zerofill": false,
                  "alias": "KillFailed",
                  "mode": "mean",
                  "queries": [
                    54
                  ],
                  "derivative": false
                }
              ],
              "pointradius": 5,
              "resolution": 100,
              "options": false,
              "interactive": true
            }
          ],
          "type": "column"
        },
        {
          "span": 4,
          "title": "..",
          "editable": true,
          "error": false,
          "loadingEditor": false,
          "panels": [
            {
              "show_query": true,
              "sizeable": false,
              "zoomlinks": false,
              "height": "220px",
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
              "linewidth": 1,
              "fill": 3,
              "loading": false,
              "span": 10,
              "title": "[2] Count of Tasks in non final States",
              "tooltip": {
                "value_type": "cumulative",
                "query_as_alias": true
              },
              "stack": false,
              "removable": false,
              "percentage": false,
              "auto_int": false,
              "type": "multifieldhistogram",
              "x-axis": true,
              "editable": true,
              "grid": {
                "max": null,
                "min": 0
              },
              "legend": true,
              "legend_counts": false,
              "y-axis": true,
              "time_field": "@timestamp",
              "interval": "5m",
              "lines": true,
              "y_format": "none",
              "draggable": false,
              "points": false,
              "values": [
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": "#64B0C8",
                  "zerofill": false,
                  "alias": "New",
                  "mode": "total",
                  "queries": [
                    42
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": "#DEDAF7",
                  "zerofill": false,
                  "alias": "Resubmit",
                  "mode": "total",
                  "queries": [
                    43
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": "#890F02",
                  "zerofill": false,
                  "alias": "Kill",
                  "mode": "total",
                  "queries": [
                    44
                  ],
                  "derivative": false
                },
                {
                  "value_field": "@message.nv_value",
                  "scale": 1,
                  "color": null,
                  "zerofill": false,
                  "alias": "Queued",
                  "mode": "total",
                  "queries": [
                    53
                  ],
                  "derivative": false
                }
              ],
              "pointradius": 5,
              "resolution": 100,
              "options": false,
              "interactive": true
            }
          ],
          "type": "column"
        },
        {
          "span": 4,
          "editable": true,
          "error": false,
          "loadingEditor": false,
          "panels": [
            {
              "sizeable": false,
              "labels": true,
              "height": "180px",
              "tmode": "terms_stats",
              "valuefield": "@message.errorCount",
              "spyable": true,
              "exclude": [],
              "sls": false,
              "size": 10,
              "style": {
                "font-size": "10pt"
              },
              "loading": false,
              "span": 10,
              "title": "glexec errors by site",
              "tilt": false,
              "arrangement": "horizontal",
              "field": "@message.siteName",
              "other": true,
              "removable": false,
              "type": "terms",
              "missing": false,
              "queries": {
                "mode": "selected",
                "ids": [
                  35
                ]
              },
              "editable": true,
              "chart": "bar",
              "counter_pos": "below",
              "value_info": true,
              "tstat": "total_count",
              "draggable": false,
              "donut": false,
              "count_column": true,
              "error": false,
              "order": "count"
            }
          ],
          "type": "column"
        }
      ]
    },
    {
      "title": "TIPS AND INFO",
      "height": "100px",
      "editable": true,
      "collapse": true,
      "collapsable": true,
      "panels": [
        {
          "error": false,
          "span": 3,
          "editable": true,
          "type": "text",
          "loadingEditor": false,
          "mode": "html",
          "content": "If the number of failed tasks increase you can check the last failures reasons here <br/> <a href=\"https://cmsweb-testbed.cern.ch/crabserver/prod/task?subresource=lastfailures&minutes=120\">Last failed tasks</a>",
          "style": {},
          "title": "TIPS"
        },
        {
          "span": 6,
          "editable": true,
          "type": "hits",
          "loadingEditor": false,
          "style": {
            "font-size": "10pt"
          },
          "arrangement": "horizontal",
          "chart": "list",
          "counter_pos": "above",
          "donut": false,
          "tilt": false,
          "labels": true,
          "spyable": true,
          "queries": {
            "mode": "selected",
            "ids": [
               15,
               19,
