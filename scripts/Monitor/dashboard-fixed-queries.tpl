          "style": {},
          "title": "schedds host metrics"
        }
      ],
      "notice": false
    },
    {
      "notice": false,
      "collapsable": true,
      "collapse": true,
      "title": "RAW DATA FOR DEV/DEBUG",
      "editable": true,
      "height": "150px",
      "panels": [
        {
          "header": true,
          "trimFactor": 300,
          "spyable": true,
          "field_list": false,
          "size": 50,
          "all_fields": false,
          "style": {
            "font-size": "9pt"
          },
          "span": 12,
          "pages": 10,
          "loadingEditor": false,
          "type": "table",
          "sort": [
            "@message.exceptstate",
            "desc"
          ],
          "error": false,
          "editable": true,
          "offset": 0,
          "overflow": "min-height",
          "normTimes": true,
          "localTime": false,
          "sortable": true,
          "fields": [
            "@fields.entity",
            "@message.nv_description",
            "@fields.metric_name",
            "@message.nv_name",
            "@message.nv_value",
            "@message.exceptstate"
          ],
          "paging": true,
          "queries": {
            "mode": "all",
            "ids": [
              0,
              1,
              2,
              3,
              4,
              5,
              6,
              7,
              8,
              9,
              10,
              11,
              12,
              13,
              14,
              15,
              16,
              17,
              18,
              19,
              20,
              21,
              22,
              23,
              24,
              25,
              26,
              27,
              28,
              29,
              30,
              31,
              32,
              33,
              34,
              35,
              36,
              37,
              38,
              39,
              40,
              41,
              42,
              43,
              44
            ]
          },
          "timeField": "@timestamp",
          "highlight": []
        }
      ]
    }
  ],
  "title": "CRAB3 Monitoring - Overview",
  "failover": false,
  "editable": true,
  "refresh": "5m",
  "loader": {
    "load_gist": true,
    "hide": false,
    "save_temp": true,
    "load_elasticsearch_size": 20,
    "load_local": true,
    "save_temp_ttl": "30d",
    "load_elasticsearch": true,
    "save_local": true,
    "save_temp_ttl_enable": true,
    "save_elasticsearch": true,
    "save_gist": true,
    "save_default": false
  },
  "pulldowns": [
    {
      "notice": false,
      "enable": true,
      "collapse": true,
      "pinned": true,
      "query": "*",
      "history": [],
      "type": "query",
      "remember": 10
    },
    {
      "notice": true,
      "enable": true,
      "type": "filtering",
      "collapse": true
    }
  ],
  "nav": [
    {
      "status": "Stable",
      "notice": false,
      "enable": true,
      "collapse": false,
      "time_options": [
        "1h",
        "2h",
        "3h",
        "4h",
        "5h",
        "6h",
        "12h",
        "1d",
        "2d",
        "3d",
        "4d",
        "5d",
        "6d",
        "7d",
        "30d"
      ],
      "refresh_intervals": [
        "2m",
        "5m",
        "10m",
        "15m",
        "30m",
        "1h",
        "3h",
        "6h",
        "12h",
        "1d",
        "2d",
        "7d",
        "30d"
      ],
      "filter_id": 0,
      "timefield": "@timestamp",
      "now": true,
      "type": "timepicker"
    }
  ],
  "services": {
    "filter": {
      "list": {
        "0": {
          "field": "@timestamp",
          "from": "now-3h",
          "alias": "",
          "to": "now",
          "mandate": "must",
          "active": true,
          "type": "time",
          "id": 0
        }
      },
      "ids": [
        0
      ],
      "idQueue": [
        3
      ]
    },
    "query": {
      "list": {
        "0": {
          "enable": true,
          "pin": true,
          "color": "#E5AC0E",
          "alias": "queued",
          "query": "vocms0118 AND @message.nv_name:\"number_of_QUEUED_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 0
        },
        "1": {
          "enable": true,
          "pin": true,
          "color": "#890F02",
          "alias": "failed",
          "query": "vocms0118 AND @message.nv_name:\"number_of_FAILED_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 1
        },
        "2": {
          "enable": true,
          "pin": true,
          "color": "#58140C",
          "alias": "killed",
          "query": "vocms0118 AND @message.nv_name:\"number_of_KILLED_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 2
        },
        "3": {
          "enable": true,
          "pin": true,
          "color": "#6ED0E0",
          "alias": "submitted",
          "query": "vocms0118 AND @message.nv_name:\"number_of_SUBMITTED_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 3
        },
        "4": {
          "enable": true,
          "pin": true,
          "color": "#E24D42",
          "alias": "new",
          "query": "vocms0118 AND @message.nv_name:\"number_of_NEW_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 4
        },
        "15": {
          "enable": true,
          "pin": true,
          "color": "#584477",
          "alias": "Prod TW",
          "query": "vocms052 AND @fields.metric_name:exception.*",
          "type": "lucene",
          "id": 15
        },
        "19": {
          "enable": true,
          "pin": true,
          "color": "#F9BA8F",
          "alias": "Pre Prod TW",
          "query": "vocms0118 AND @fields.metric_name:exception.*",
          "type": "lucene",
          "id": 19
        },
        "35": {
          "enable": true,
          "pin": true,
          "color": "#F9934E",
          "alias": "glexecErrors",
          "query": "@fields.metric_id: \"13242\"",
          "type": "lucene",
          "id": 35
        },
        "42": {
          "enable": true,
          "pin": true,
          "color": "#2F575E",
          "alias": "tmpNew",
          "query": "vocms0118 AND @message.nv_name:\"numberOfTasksIn_NEW_State\"",
          "type": "lucene",
          "id": 42
        },
        "43": {
          "enable": true,
          "pin": true,
          "color": "#99440A",
          "alias": "tmpResubmit",
          "query": "vocms0118 AND @message.nv_name:\"numberOfTasksIn_RESUBMIT_State\"",
          "type": "lucene",
          "id": 43
        },
        "44": {
          "enable": true,
          "pin": true,
          "color": "#58140C",
          "alias": "tmpKill",
          "query": "vocms0118 AND @message.nv_name:\"numberOfTasksIn_KILL_State\"",
          "type": "lucene",
          "id": 44
        },
        "53": {
          "enable": true,
          "pin": true,
          "color": "#E5AC0E",
          "alias": "tmpQueued",
          "query": "vocms0118 AND @message.nv_name:\"numberOfTasksIn_QUEUED_State\"",
          "type": "lucene",
          "id": 53
        },
        "54": {
          "enable": true,
          "pin": true,
          "color": "#F9D9F9",
          "alias": "KILLFAILED",
          "query": "vocms0118 AND @message.nv_name:\"number_of_KILLFAILED_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 54
        },
        "55": {
          "enable": true,
          "pin": true,
          "color": "#DEDAF7",
          "alias": "SUBMITFAILED",
          "query": "vocms0118 AND @message.nv_name:\"number_of_SUBMITFAILED_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 55
        },
        "56": {
          "enable": true,
          "pin": true,
          "color": "#7EB26D",
          "alias": "RESUBMITFAILED",
          "query": "vocms0118 AND @message.nv_name:\"number_of_RESUBMITFAILED_tasks_in_the_last_minute\"",
          "type": "lucene",
          "id": 56
        },

