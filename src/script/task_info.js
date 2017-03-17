$(document).ready(function() {
    var transferInfo_loaded = false;
    // initialize tooltips
    $(function () {
        $('[data-toggle="tooltip"]').tooltip()
    })
    // initialize table for transfers
    $('#transfer-table tfoot th').each( function () {
        var title = $(this).text();
        $(this).html( '<input type="text" placeholder="'+title+'" />' );
    } );

    var transtable= $("#transfer-table").DataTable({
        colReorder: true,
        fixedHeader: true,
        autoWidth: false,
        });

    // initialize table for document 
    var doctable= $("#doc-table").DataTable({"dom": '<"top"i>rt<"bottom"flp><"clear">',
                                             "paging":   false,
                                             "ordering": false,
                                             "info":     false
                                            });

    // load transfer tab only on tab click
    // to avoid slowing down the response
    // for other tabs
    $('.nav-tabs a').click(function(){
        if(this.id=='transferTab' && !transferInfo_loaded){
            setupLoading();
            displayTransferInfo(handleTransferInfoErr);       
            transferInfo_loaded = true;

        }
    });

    // setup spinning animation for transfer info table
    function setupLoading() {    
        $('<div id="divSpin" />').appendTo(document.body);

        var target = document.getElementById("divSpin");

        var opts = {
            lines: 13, // The number of lines to draw
            length: 20, // The length of each line
            width: 10, // The line thickness
            radius: 30, // The radius of the inner circle
            corners: 1, // Corner roundness (0..1)
            rotate: 8, // The rotation offset
            direction: 1, // 1: clockwise, -1: counterclockwise
            color: '#000', // #rgb or #rrggbb or array of colors
            speed: 1, // Rounds per second
            trail: 60, // Afterglow percentage
            shadow: false, // Whether to render a shadow
            hwaccel: false, // Whether to use hardware acceleration
            className: 'mySpin', // The CSS class to assign to the spinner
            zIndex: 2e9, // The z-index (defaults to 2000000000)
            top: '50%', // Top position relative to parent
            left: '50%' // Left position relative to parent
        };

        mySpinner = new Spinner(opts).spin(target);
    };

    function removeLoading(){
        mySpinner.stop();
    }

    function showLoading() {
        mySpinner.spin();
    }


    // set datatable X scroll
    jQuery('.dataTable').wrap('<div class="dataTables_scroll" />');

    // Task name that was entered by the user, is set on form submission
    var inputTaskName = "";

    var DB_VERSIONS = ["prod", "preprod", "dev"];

    // Task info is stored upon displaying it. Required for loading related information about a task.
    var taskInfo = "",
        dbVersion = "",
        taskInfoUrl = "",
        taskStatusUrl = "",
        cacheUrl = "",
        webDirProxyApiUrl = "",
        username = "",
        userWebDir = "",
        scriptExe = "",
        inputDataset = "",
        outputDataset = "",
        proxiedWebDirUrl = "",
        dbsInstance = "",
        dbsPublicationInstance = "";


    // If a parameter "task" exists in the URL, tries to load task info the same way a form submit loads it.
    processPageUrl();
    /**
     * Task search form listener - the starting point of control flow.
     */
    $("#task-search-form").submit(function(e) {
        e.preventDefault();

        //Trimming whitespaces from the search field
        inputTaskName = $("#task-search-form-input").val().trim();
        $("#task-search-form-input").val(inputTaskName);

        dbVersion = $("#db-selector-box").val();
        setUrls(dbVersion);

        taskInfo = "";

        // Change the URL so that it can be copied/pasted more easily
        var temp = window.location.href.split("/ui")[0] + "/ui/task/" + encodeURIComponent(inputTaskName);

        // If default db version is overriden, add it to url parameters
        if (getDbVersionSelector() !== getDefaultDbVersion()) {
            temp += "/dbver/" + getDbVersionSelector();
        }

        window.history.pushState("", "", temp);

        clearPreviousContent();
        displayTaskInfo(handleTaskInfoErr);
        transferInfo_loaded = false;
    });

    /**
     * Task search form clear button listener - clears the field when the button
     * is pressed.
     */
    $("#clear-button").click(function() {
        $("#task-search-form-input").val("");
    })

    /**
     * Saves necessary information from task info into global variables
     * Has to be run after displayTaskInfo
     */
    function loadGlobalDataFromTaskInfo() {
        userWebDir = "", username = "", cacheUrl = "", scriptExe = "", inputDataset = "",
        outputDataset = "";

        if (taskInfo != undefined && taskInfo != "") {
            for (var i = 0; i < taskInfo.desc.columns.length; i++) {

                switch (taskInfo.desc.columns[i]) {
                    case "tm_user_webdir":
                        userWebDir = taskInfo.result[i];
                        break;
                    case "tm_username":
                        username = taskInfo.result[i];
                        break;
                    case "tm_cache_url":
                        cacheUrl = taskInfo.result[i];
                        break;
                    case "tm_scriptexe":
                        scriptExe = taskInfo.result[i];
                        break;
                    case "tm_input_dataset":
                        inputDataset = taskInfo.result[i];
                        break;
                    case "tm_dbs_url":
                        // RegExp to extract the DBS instance
                        re = /(\/cmsweb.cern.ch\/dbs\/)(.+)(\/DBSReader)/;
                        dbsInstance = taskInfo.result[i].match(re)[2];
                        break;
                    case "tm_publish_dbs_url":
                        // RegExp to extract the DBS publication instance
                        re = /(\/cmsweb.cern.ch\/dbs\/)(.+)(\/DBSWriter)/;
                        dbsPublicationInstance = taskInfo.result[i].match(re)[2];
                        break;
                    case "tm_output_dataset":
                        // Extract the output dataset name (/A/B/C) from a string like this:
                        // ['/A/B/C']
                        re = /\['(.+)'\]/;
                        if (taskInfo.result[i] !== "None") {
                            outputDataset = taskInfo.result[i].match(re)[1];
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Called on task info search form submission. It then fetches JSON data
     * and inserts it into the #task-info-table.
     */
    function displayTaskInfo(errHandler) {
        var xmlhttp = new XMLHttpRequest();
        // var url = "https://mmascher-mon.cern.ch/crabserver/dev/task?subresource=search&workflow=";
        var url = taskInfoUrl + inputTaskName;
        
        function queryApi(url) {
            $.ajax(url)
                .done(function(data) {
                    // Storing the data for the use of other display functions
                    taskInfo = data;

                    // Creating table contents
                    for (i = 0; i < data.desc.columns.length; i++) {
                        $("#task-info-table tbody")
                            .append("<tr><td>" + data.desc.columns[i] + "</td><td>" + data.result[i] + "</td></tr>");
                    }
                    loadOtherData();

                })
                .fail(function(xhr) {
                    var headers = xhr.getAllResponseHeaders().toLowerCase();
                    errHandler(new ServerError(headers));
                    loadOtherData();
                })
        };
        queryApi(url);
    }

    /**
     * Fetches and displays the config/ PSet files for given task.
     * It first queries an api which either returns a proxied url (which is needed to get around firewalls)
     * or returns nothing, in which case the files cannot be retrieved. In that case the links are pointed to the sandbox, because it is possible that the task was not submitted to the schedd and the debug directory doesn't exist.
     *
     * If the proxy is found, displays the file content and sets direct links to the files.
     */
    function displayConfigAndPSet(errHandler) {
        if (userWebDir === "") {
            errHandler(new TaskInfoUndefinedError());
            return;
        } else if (userWebDir === "None") {
            // If user webdir wasn't created at all
            errHandler(new UserWebDirUndefinedError());
            return;
        } else if (proxiedWebDirUrl === "") {
            // In case proxy api returned empty or failed
            // Set links, show error and don't load anything else.
            $("#task-config-link").attr("href", userWebDir + "/debug_files.tar.gz");
            $("#task-pset-link").attr("href", userWebDir + "/debug_files.tar.gz");
            errHandler(new ProxyNotFoundErrorError);
            return;
        }

        $.ajax(proxiedWebDirUrl + "/debug/crabConfig.py")
            .done(function(data) {
                $("#task-config-paragraph").text(data);
            });

        $.ajax(proxiedWebDirUrl + "/debug/originalPSet.py")
            .done(function(data) {
                $("#task-pset-paragraph").text(data);
            });

        $("#task-config-link").attr("href", proxiedWebDirUrl + "/debug/crabConfig.py");
        $("#task-pset-link").attr("href", proxiedWebDirUrl + "/debug/originalPSet.py");
    }

    function queryWebDirProxyApi() {
        $.ajax(webDirProxyApiUrl + inputTaskName)
            .done(function(data) {
                proxiedWebDirUrl = data.result[0];

                if (proxiedWebDirUrl === undefined || proxiedWebDirUrl == "None") {
                    proxiedWebDirUrl = "";
                }
            })
            .fail(function(xhr) {
                proxiedWebDirUrl = "";
            })
            .complete(function(xhr) {
                displayConfigAndPSet(handleConfigPSetErr);
                displayScriptExe(handleScriptExeErr);
                displayMainPage(handleMainErr);
            });
    }

    /**
     * Fetches and displays TaskWorker log for given task
     */
    function displayTaskWorkerLog(errHandler) {
        var xmlhttp = new XMLHttpRequest();

        if (cacheUrl === undefined || cacheUrl === "") {
            errHandler(new TaskInfoUndefinedError());
            return;
        }

        var url = cacheUrl + "/logfile?name=" + inputTaskName + "_TaskWorker.log&username=" + username;

        function queryApi(url) {
            $.ajax(url)
                .done(function(data) {
                    $("#taskworker-log-paragraph").text(data);
                })
                .fail(function(xhr) {
                    var headers = xhr.getAllResponseHeaders().toLowerCase();
                    errHandler(new ServerError(headers));
                });
        }
        $("#taskworker-log-link").attr("href", url);
        queryApi(url);
    }

    /**
     * Fetches and displays upload log for a given task. This file is created with "crab uploadlog" command
     * and therefore doesn't always exist.
     */
    function displayUploadLog(errHandler) {

        if (cacheUrl === undefined || cacheUrl === "") {
            errHandler(new TaskInfoUndefinedError());
            return;
        }

        var url = cacheUrl + "/logfile?name=" + inputTaskName + ".log&username=" + username;

        function queryApi(url) {
            $.ajax(url)
                .done(function(data) {
                    $("#upload-log-paragraph").text(data);
                })
                .fail(function(xhr) {
                    var headers = xhr.getAllResponseHeaders().toLowerCase();
                    errHandler(new ServerError(headers));
                });
        }
        $("#upload-log-link").attr("href", url);
        queryApi(url);
    }

    /**
     * Fetches and displays a script file which the user can choose to submit with his task.
     */
    function displayScriptExe(errHandler) {

        if (scriptExe === undefined || scriptExe === "") {
            errHandler(new TaskInfoUndefinedError);
            return;
        } else if (scriptExe === "None") {
            errHandler(new ScriptExeNotUsedError);
            return;
        } else if (userWebDir === "None") {
            // If user webdir wasn't created at all
            errHandler(new UserWebDirUndefinedError());
            return;
        } else if (proxiedWebDirUrl === "") {
            // In case proxy api returned empty or failed
            // Set links, show error and don't load anything else.
            $("#script-exe-link").attr("href", userWebDir + "/sandbox.tar.gz");
            errHandler(new ProxyNotFoundErrorError);
            return;
        }

        $.ajax(proxiedWebDirUrl + "/debug/" + scriptExe)
            .done(function(data) {
                $("#script-exe-paragraph").text(data);
            });

        $("#script-exe-link").attr("href", proxiedWebDirUrl + "/debug/" + scriptExe);
    }

    /**
     * Called on task info search form submission. It then fetches JSON data
     * inserts it into the #transfer-info-table.
     */
    function displayTransferInfo(errHandler) {
        var xmlhttp = new XMLHttpRequest();
        // var url = "https://asotest3.cern.ch/crabserver/dev/fileusertransfers?subresource=getTransferStatus&taskname=";
        var url = transferInfo + inputTaskName + "&username=" + username;
        var get_doc_url = docInfo
        var tr_state = ["NEW", "ACQUIRED", "FAILED", "DONE", "RETRY", "SUBMITTED", "KILL", "KILLED"]

        function label(state) {
           switch (state) {
               case "NEW":
               case "ACQUIRED":
                   return "<span class='label label-info'>" + state + "</span>"
               case "SUBMITTED":
                   return "<span class='label label-warning'>" + state + "</span>"
               case "FAILED":
               case "RETRY":
                   return "<span class='label label-danger'>" + state + "</span>"
               case "DONE":
                   return "<span class='label label-success'>" + state + "</span>"
               case "KILL":
               case "KILLED":
                   return "<span class='label label-default'>" + state + "</span>"

           }
        };

        function queryApi(url) {
            $.ajax(url)
                .done(function(data) {
                    // Storing the data for the use of other display functions
                    taskInfo = data;
                    // Creating table contents
                    var index = {}
                    var content =[] 

                    for (i = 0; i < data.desc.columns.length; i++) {
                        index[data.desc.columns[i]] = i
                    }
                  

                    var duration = 0
                    var doc_url = ''
                    var doc_href = ''

                    var state = {DONE:0,
                                 NEW:0,
                                 ACQUIRED:0,
                                 SUBMITTED:0,
                                 RETRY:0,
                                 FAILED:0,
                                 KILL:0,
                                 KILLED:0,
                        }
                    transtable.clear();
                    transtable.colReorder.reset();
                    for (k = 0; k < data.result.length; k++) {
                        doc_url = get_doc_url + data.result[k][index["tm_id"]]
                        doc_href = "<a href=" + doc_url + "> " + data.result[k][index["tm_id"]] + " </a>"
                        fts_href = null
                        if (data.result[k][index["tm_fts_instance"]]){
                            fts_href = "<a href=" + 
                                    data.result[k][index["tm_fts_instance"]].replace("8446","8449") + 
                                    "/fts3/ftsmon/#/job/" + 
                                    data.result[k][index["tm_fts_id"]] + "> " + 
                                    data.result[k][index["tm_fts_id"]] + 
                                " </a>"
                        }
                        content = [
                            data.result[k][index["tm_jobid"]],
                            data.result[k][index["tm_id"]], 
                            label(tr_state[data.result[k][index["tm_transfer_state"]]]),
                            fts_href,
                        ]
                        state[tr_state[data.result[k][index["tm_transfer_state"]]]] += 1
                        duration = null
                        if(data.result[k][index["tm_transfer_state"]] == 3){
                           var duration = (data.result[k][index["tm_last_update"]] - data.result[k][index["tm_start_time"]])/60
                        }
                        
                        if (duration) content.push(duration.toFixed(0))
                        content.push(duration)

                        transtable.row.add( content ).draw()

                    }

                    transtable.columns.adjust().draw();

                    // update progress bar
                    if(data.result.length > 0){
                        var percentage = 100*state["DONE"]/parseFloat(data.result.length)
                        $('#completed').width(percentage+"%")
                        percentage = 100*state["NEW"]/parseFloat(data.result.length)
                        percentage += 100*state["ACQUIRED"]/parseFloat(data.result.length)
                        $('#new').width(percentage+"%")
                        percentage = 100*state["SUBMITTED"]/parseFloat(data.result.length)
                        $('#submitted').width(percentage+"%")
                        percentage = 100*state["RETRY"]/parseFloat(data.result.length)
                        percentage += 100*state["FAILED"]/parseFloat(data.result.length)
                        $('#failed').width(percentage+"%")
                        percentage = 100*state["KILL"]/parseFloat(data.result.length)
                        percentage += 100*state["KILLED"]/parseFloat(data.result.length)
                        $('#killed').width(percentage+"%")
                    }

                removeLoading();

                })
                .fail(function(xhr) {
                    var headers = xhr.getAllResponseHeaders().toLowerCase();
                    errHandler(new ServerError(headers));
                    removeLoading();
                })
        };
        
        queryApi(url);
    }

    $('#transfer-table tbody').on('click', 'tr', function () {
        var data = transtable.row( this ).data();
        url = docInfo + data[1]

        function queryApi(url) {
            $.ajax(url)
                .done(function(data) {
                    // Creating table contents
                    var content = '' 

                    doctable.clear();
                    for (i = 0; i < data.desc.columns.length; i++) {
                             doctable.row.add( [data.desc.columns[i],data.result[0][i]] ).draw()
                    }

                })
                .fail(function(xhr) {
                    var headers = xhr.getAllResponseHeaders().toLowerCase();
                    errHandler(new ServerError(headers));
                })

        };
        queryApi(url);

    } );

    /**
     * Add filter for each column
     */

    transtable.columns().every( function () {
        var that = this;
 
        $( 'input', this.footer() ).on( 'keyup change', function () {
            if ( that.search() !== this.value ) {
                that
                    .search( this.value )
                    .draw();
            }
        } );

        // set searchbox width
        $( 'input', this.footer() ).each( function () {
            if(this.placeholder=="File ID") this.style["width"] = "300px";
            else if(this.placeholder=="Transfer State") this.style["width"] = "120px";
            else if(this.placeholder=="Duration [min]") this.style["width"] = "120px";
            else if(this.placeholder=="") this.style["width"] = "120px";
        
         }
        );

    } );
    /**
     * Displays main page information - sets correct links and loads task status
     */
    function displayMainPage(errHandler) {
        if (userWebDir !== "" && inputTaskName !== "" && inputTaskName !== undefined) {

            var dashboardUrl = "http://dashb-cms-job.cern.ch/dashboard/templates/" +
                "task-analysis/#user=" + username + "&table=Mains&pattern=" + inputTaskName;

            var dasInputUrl = "https://cmsweb.cern.ch/das/request?view=list&limit=50&instance=" + dbsInstance + "&input=" + inputDataset;

            if (outputDataset !== "") {
                var dasOutputUrl = "https://cmsweb.cern.ch/das/request?instance=" + dbsPublicationInstance + "&input=" + outputDataset;
                $("#main-das-link-output").removeClass("disabled");
                $("#main-das-link-output").attr("href", dasOutputUrl);
                $("#main-das-link-output").text("DAS Output");
            }

            $("#main-dashboard-link").attr("href", dashboardUrl);

            webDirUrlToDisplay = "";
            if (proxiedWebDirUrl === "") {
                webDirUrlToDisplay = userWebDir;
            } else {
                // Adding a slash at the end to ensure redirection works correctly
                // https://hypernews.cern.ch/HyperNews/CMS/get/webInterfaces/1360.html
                webDirUrlToDisplay = proxiedWebDirUrl + "/";
            }

            $("#main-webdir-link").attr("href", webDirUrlToDisplay);
            $("#main-das-link-input").attr("href", dasInputUrl);

            var url = taskStatusUrl + inputTaskName;

            /**
             * Displays the table for the main task info page.
             *
             * @param {Object} obj - Result from the api query to be processed
             */
            function printOutput(obj) {
                for (var key in obj) {
                    var attrName = key;
                    var attrValue = obj[key];

                    // Custom formatting for "jobList" information
                    sumObject = {};
                    if (attrName === "jobList") {
                        for (var i = 0; i < obj["jobList"].length; i++) {
                            // Take the job status
                            jobStatus = obj["jobList"][i][0];
                            // and add it to the total status count.
                            if (jobStatus in sumObject) {
                                sumObject[jobStatus]++;
                            } else {
                                sumObject[jobStatus] = 1;
                            }
                        }
                        attrValue = '<table style="width:10%">'
                        for (var foundStatus in sumObject) {
                            attrValue += "<tr><td>" + foundStatus + " </td><td> " + sumObject[foundStatus] + "</td></tr>";
                        }
                        attrValue += "<tr><td>TOTAL </td><td> " + obj["jobList"].length + "</td></tr>";
                        attrValue += "</table>"
                    }
                    $("#main-status-info-table tbody.dynamic-content")
                        .append("<tr><td>" + attrName + "</td><td>" + attrValue + "</td></tr>");
                }
            }

            function queryApi(url) {
                $.ajax(url)
                    .done(function(data) {
                        var obj = data.result[0];
                        printOutput(obj);
                    })
            }
            queryApi(url);
        } else {
            errHandler(new TaskInfoUndefinedError);
        }
    }

    /**
     * Splits the header string and returns an array with only the interesting headers
     *
     * @param  {String} The string with all the response headers
     * @return {Array} Array of header strings with some of them removed.
     */
    function processErrorHeaders(headers) {
        var headerArray = headers.split("\r\n");
        var resultArray = [];

        for (var i = 0; i < headerArray.length; i++) {
            var str = headerArray[i];
            if (str.search("x-error-http") != -1 || str.search("x-error-info") != -1 || str.search("x-rest-status") != -1 ||
                str.search("x-error-detail") != -1 || str.search("x.error-id") != -1) {

                resultArray.push(str);
            }
        }
        return resultArray;
    }

    function handleUploadLogErr(err) {
        if (err instanceof TaskInfoUndefinedError) {
            $("#upload-log-error-box").css("display", "inherit").text("Task info not loaded");
        } else if (err instanceof ServerError) {
            $("#upload-log-error-box").empty().css("display", "inherit");
            var headers = err.headers;

            if (headers != undefined && headers != "") {
                var headerArray = processErrorHeaders(headers);
                for (var i = 0; i < headerArray.length; i++) {
                    var colonIndex = headerArray[i].search(":");
                    $("#upload-log-error-box").append("<span id=\"spaced-span\">" + headerArray[i].substr(0, colonIndex + 1) + "</span><span>" + headerArray[i].substr(colonIndex + 1) + "</span><br/>");
                }
            } else {
                $("#upload-log-error-box").css("display", "inherit").text("Couldn't load UploadLog, please try the link below.");
            }
        }
    }

    function handleTaskInfoErr(err) {
        $("#task-info-error-box").empty().css("display", "inherit");

        var headers = err.headers;
        var headerArray = processErrorHeaders(headers);
        for (var i = 0; i < headerArray.length; i++) {
            var colonIndex = headerArray[i].search(":");
            $("#task-info-error-box").append("<span id=\"spaced-span\">" + headerArray[i].substr(0, colonIndex + 1) + "</span><span>" + headerArray[i].substr(colonIndex + 1) + "</span><br/>");
        }
    }


    function handleTransferInfoErr(err) {
        $("#transfer-info-error-box").empty().css("display", "inherit");

        var headers = err.headers;
        var headerArray = processErrorHeaders(headers);
        for (var i = 0; i < headerArray.length; i++) {
            var colonIndex = headerArray[i].search(":");
            $("#transfer-info-error-box").append("<span id=\"spaced-span\">" + headerArray[i].substr(0, colonIndex + 1) + "</span><span>" + headerArray[i].substr(colonIndex + 1) + "</span><br/>");
        }
    }

    function handleTaskWorkerLogErr(err) {

        if (err instanceof InvalidQueryError) {
            // This is when it is impossible to determine a username from the search query.
            // No point in sending a request to server with a null username.
            $("#taskworker-log-error-box").css("display", "inherit").text("Invalid query");
        } else if (err instanceof ServerError) {
            // When a server response code is not equal to 200 - something went wrong.

            $("#taskworker-log-error-box").empty().css("display", "inherit");
            var headers = err.headers;

            if (headers != undefined && headers != "") {
                var headerArray = processErrorHeaders(headers);

                for (var i = 0; i < headerArray.length; i++) {
                    var colonIndex = headerArray[i].search(":");
                    $("#taskworker-log-error-box").append("<span id=\"spaced-span\">" + headerArray[i].substr(0, colonIndex + 1) + "</span><span>" + headerArray[i].substr(colonIndex + 1) + "</span>\n");
                }
            } else {
                $("#taskworker-log-error-box").css("display", "inherit").text("Couldn't load TaskWorker log, please try the link below.");
            }
        } else if (err instanceof TaskInfoUndefinedError) {
            $("#taskworker-log-error-box").empty().css("display", "inherit").text("Task info not loaded");
        }
    }

    function handleConfigPSetErr(err) {
        if (err instanceof ProxyNotFoundErrorError) {
            $("#task-config-error-box").css("display", "inherit").text("Couldn't load config, please try the link below.");
            $("#task-pset-error-box").css("display", "inherit").text("Couldn't load PSet, please try the link below.");
        } else if (err instanceof TaskInfoUndefinedError) {
            $("#task-config-error-box").css("display", "inherit").text("Task Info not loaded, can't get config");
            $("#task-pset-error-box").css("display", "inherit").text("Task Info not loaded, can't get PSet")
        } else if (err instanceof UserWebDirUndefinedError) {
            $("#task-config-error-box").css("display", "inherit").text("Task webdir is not available (maybe the task was not submitted to the schedd)");
            $("#task-pset-error-box").css("display", "inherit").text("Task webdir is not available (maybe the task was not submitted to the schedd)")
        }
    }

    function handleScriptExeErr(err) {
        if (err instanceof ScriptExeNotUsedError) {
            $("#script-exe-error-box").css("display", "inherit").text("ScriptExe was not used");
        } else if (err instanceof ProxyNotFoundErrorError) {
            $("#script-exe-error-box").css("display", "inherit").text("Couldn't load ScriptExe, please try the link below.");
        } else if (err instanceof TaskInfoUndefinedError) {
            $("#script-exe-error-box").css("display", "inherit").text("Task info not loaded");
        } else if (err instanceof UserWebDirUndefinedError) {
            $("#script-exe-error-box").css("display", "inherit").text("Task webdir is not available (maybe the task was not submitted to the schedd)");
        }
    }

    function handleScriptExeCallbackErr(err){
        $("#script-exe-error-box").css("display", "inherit").text(err ? err : xhr.status);
    }

    function handleMainErr(err) {
        $("#main-error-box").css("display", "inherit").text("Task info not loaded");
    }

    function ServerError(headers) {
        this.headers = headers;
        this.name = "ServerError";
    }

    function InvalidQueryError(headers) {
        this.headers = headers;
        this.name = "InvalidQueryError";
    }

    function TaskInfoUndefinedError() {
        this.name = "TaskInfoUndefinedError";
    }

    function ScriptExeNotUsedError() {
        this.name = "ScriptExeNotUsedError";
    }

    function ProxyNotFoundErrorError() {
        this.name = "ProxyNotFoundErrorError";
    }

    function UserWebDirUndefinedError() {
        this.name = "UserWebDirUndefinedError";
    }

    function setUrls(dbVersion) {
        switch (dbVersion) {
            case "prod":
                taskInfoUrl = "https://cmsweb.cern.ch/crabserver/prod/task?subresource=search&workflow=";
                taskStatusUrl = "https://cmsweb.cern.ch/crabserver/prod/workflow?workflow=";
                webDirProxyApiUrl = "https://cmsweb.cern.ch/crabserver/prod/task?subresource=webdirprx&workflow="
                transferInfo = "https://cmsweb.cern.ch/crabserver/prod/fileusertransfers?subresource=getTransferStatus&taskname="
                docInfo = "https://" + document.domain + "/crabserver/prod/fileusertransfers?subresource=getById&id="
                break;
            case "preprod":
                taskInfoUrl = "https://" + document.domain + "/crabserver/preprod/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/preprod/workflow?workflow=";
                webDirProxyApiUrl = "https://" + document.domain + "/crabserver/preprod/task?subresource=webdirprx&workflow="
                transferInfo = "https://" + document.domain + "/crabserver/preprod/fileusertransfers?subresource=getTransferStatus&taskname="
                docInfo = "https://" + document.domain + "/crabserver/preprod/fileusertransfers?subresource=getById&id="
                break;
            case "dev":
                taskInfoUrl = "https://" + document.domain + "/crabserver/dev/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/dev/workflow?workflow=";
                webDirProxyApiUrl = "https://" + document.domain + "/crabserver/dev/task?subresource=webdirprx&workflow="
                transferInfo = "https://" + document.domain + "/crabserver/dev/fileusertransfers?subresource=getTransferStatus&taskname="
                docInfo = "https://" + document.domain + "/crabserver/dev/fileusertransfers?subresource=getById&id="
                break;
            default:
                break;
        }
    }

    function setDefaultDbVersionSelector() {
        switch (document.domain) {
            case "cmsweb.cern.ch":
                $("#db-selector-box").val("prod");
                break;
            case "cmsweb-testbed.cern.ch":
                $("#db-selector-box").val("preprod");
                break;
            default:
                $("#db-selector-box").val("dev")
                break;
        }
    }

    function getDbVersionSelector() {
        return $("#db-selector-box").val();
    }

    function getDefaultDbVersion() {
        switch (document.domain) {
            case "cmsweb.cern.ch":
                return "prod";
            case "cmsweb-testbed.cern.ch":
                return "preprod";
            default:
                return "dev";
        }
    }

    /**
     * Processes a page url based on the parameters it contains.
     * If a /task/<taskname> parameter is found, tries to load a task from the database
     * If a /dbver/<dbversion> parameter is found, sets the specified db version to use in queries
     */
    function processPageUrl() {
        var urlArray = window.location.href.split("\/");
        var taskIndex = $.inArray("task", urlArray);

        var dbVersionIndex = $.inArray("dbver", urlArray);

        if (dbVersionIndex != -1 && urlArray.length > dbVersionIndex && urlArray[dbVersionIndex + 1] !== ""
                && $.inArray(urlArray[dbVersionIndex + 1], DB_VERSIONS) !== -1) {
            dbVersion = urlArray[dbVersionIndex + 1];
            $("#db-selector-box").val(dbVersion);
        } else {
            setDefaultDbVersionSelector();
            dbVersion = getDbVersionSelector();
        }

        if (taskIndex != -1 && urlArray.length > taskIndex && urlArray[taskIndex + 1] !== "") {
            inputTaskName = decodeURIComponent(urlArray[taskIndex + 1]);
            $("#task-search-form-input").val(inputTaskName);

            setUrls(dbVersion);
            clearPreviousContent();
            displayTaskInfo(handleTaskInfoErr);
            //displayTransferInfo(handleTransferInfoErr);
        } else {
            dbVersion = getDbVersionSelector();
            setUrls(dbVersion);
        }
    }

    function loadOtherData() {
        loadGlobalDataFromTaskInfo();
        queryWebDirProxyApi();
        // displayConfigAndPSet(handleConfigPSetErr);
        displayTaskWorkerLog(handleTaskWorkerLogErr);
        displayUploadLog(handleUploadLogErr);
        //displayTransferInfo(handleTransferInfoErr);
        //displayScriptExe(handleScriptExeErr);

    }

    function clearPreviousContent() {
        $("#main-dashboard-link").attr("href", "#");
        $("#main-webdir-link").attr("href", "#");
        $("#main-das-link-input").attr("href", "#");

        $("#main-das-link-output").addClass("disabled");
        $("#main-das-link-output").attr("href", "#");
        $("#main-das-link-output").text("Publication info unavailable");


        $("#taskworker-log-link").attr("href", "#");
        $("#upload-log-link").attr("href", "#");

        $(".alert, .alert-warning").empty().css("display", "none");
        $(".dynamic-content").empty();
    }

});
