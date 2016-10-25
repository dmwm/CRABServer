$(document).ready(function() {

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
        proxiedWebDirUrl = "",
        dbsInstance = "";

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
        var temp = window.location.href.split("/ui")[0] + "/ui/task/" + inputTaskName;

        // If default db version is overriden, add it to url parameters
        if (getDbVersionSelector() !== getDefaultDbVersion()) {
            temp += "/dbver/" + getDbVersionSelector();
        }

        window.history.pushState("", "", temp);

        clearPreviousContent();
        displayTaskInfo(handleTaskInfoErr);
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
        userWebDir = "", username = "", cacheUrl = "", scriptExe = "", inputDataset = "";

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

                displayConfigAndPSet(handleConfigPSetErr);
                displayScriptExe(handleScriptExeErr);
            })
            .fail(function(xhr) {
                proxiedWebDirUrl = "";
                displayConfigAndPSet(handleConfigPSetErr);
                displayScriptExe(handleScriptExeErr);
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
     * Displays main page information - sets correct links and loads task status.
     */
    function displayMainPage(errHandler) {
        if (userWebDir !== "" && inputTaskName !== "" && inputTaskName !== undefined) {

            var dashboardUrl = "http://dashb-cms-job.cern.ch/dashboard/templates/" +
                "task-analysis/#user=" + username + "&table=" + inputTaskName + "$table=Mains&pattern=" + inputTaskName;

            var dasUrl = "https://cmsweb.cern.ch/das/request?view=list&limit=50" + "&instance=" + dbsInstance + "&input=" + inputDataset;

            $("#main-dashboard-link").attr("href", dashboardUrl);

            webDirUrlToDisplay = "";
            if (proxiedWebDirUrl === "") {
                webDirUrlToDisplay = userWebDir;
            } else {
                webDirUrlToDisplay = proxiedWebDirUrl;
            }
            $("#main-webdir-link").attr("href", webDirUrlToDisplay);
            $("#main-das-link").attr("href", dasUrl);

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
                taskInfoUrl = "https://" + document.domain + "/crabserver/prod/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/prod/workflow?workflow=";
                webDirProxyApiUrl = "https://" + document.domain + "/crabserver/prod/task?subresource=webdirprx&workflow="
                break;
            case "preprod":
                taskInfoUrl = "https://" + document.domain + "/crabserver/preprod/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/preprod/workflow?workflow=";
                webDirProxyApiUrl = "https://" + document.domain + "/crabserver/preprod/task?subresource=webdirprx&workflow="
                break;
            case "dev":
                taskInfoUrl = "https://" + document.domain + "/crabserver/dev/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/dev/workflow?workflow=";
                webDirProxyApiUrl = "https://" + document.domain + "/crabserver/dev/task?subresource=webdirprx&workflow="
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
            inputTaskName = urlArray[taskIndex + 1];
            $("#task-search-form-input").val(inputTaskName);

            setUrls(dbVersion);
            clearPreviousContent();
            displayTaskInfo(handleTaskInfoErr);
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
        // displayScriptExe(handleScriptExeErr);
        displayMainPage(handleMainErr);
    }

    function clearPreviousContent() {
        $("#taskworker-log-link").attr("href", "#");
        $("#upload-log-link").attr("href", "#");

        $(".alert, .alert-warning").empty().css("display", "none");
        $(".dynamic-content").empty();
    }
});
