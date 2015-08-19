$(document).ready(function() {
    // Task name that was entered by the user, is set on form submission
    var inputTaskName = "";

    // Task info is stored upon displaying it. Required for the tm_user_webdir value, which is needed
    // for loading the config and pset files.
    var taskInfo = "",
        dbVersion = "",
        taskInfoUrl = "",
        taskStatusUrl = "",
        cacheUrl = "",
        sandboxApiUrl = "",
        username = "",
        userWebDir = "",
        scriptExe = "",
        inputDataset = "";

    // In most cases the user won't want to override the default database
    setDefaultDbVersionSelector();

    // If a parameter "task" exists, tries to load task info the same way a form submit loads it.
    processPageUrl();


    /**
     * Task search form listener - the starting point of control flow.
     */
    $("#task-search-form").submit(function(e) {
        e.preventDefault();
        inputTaskName = $("#task-search-form-input").val();
        dbVersion = $("#db-selector-box").val();

        setUrls(dbVersion);

        taskInfo = "";

        // Change the URL so that it can be copied/pasted more easily
        var temp = window.location.href.split("/ui/")[0] + "/ui/task/" + inputTaskName;
        //window.location.href = temp;

        // TODO - is this hack ok?
        window.history.pushState("", "", temp);
        console.log("test");

        clearPreviousContent();
        displayTaskInfo(handleTaskInfoErr);
        // loadContent();

        // displayUploadLog();
        //document.location.hash = "/task/" + inputTaskName;
    });

    // Has to be run after displayTaskInfo
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
                    default:
                        break;
                }
            }
        }

    }

    /**
     * Called on task info search form submission. It then fetches JSON data
     * and inserts it into the task_info.html #task-info-table.
     */
    function displayTaskInfo(errHandler) {
        var xmlhttp = new XMLHttpRequest();
        // var url = "https://mmascher-mon.cern.ch/crabserver/dev/task?subresource=search&workflow=";
        var url = taskInfoUrl + inputTaskName;
        console.log(url);



        function queryApi(url) {
            $.ajax(url)
                .done(function(data) {
                    console.log("TW info:");
                    console.log(data);
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
                    console.log("xhr status:" + xhr.status);
                    loadOtherData();
                })
        };
        queryApi(url);
    }

    /**
     * Fetches and displays the config/ PSet files for given task.
     * It first querys an api which either returns a proxied url (which is needed to get around firewalls)
     * or returns nothing, in which case no proxying is needed.
     *
     * For getting the files out of the sandbox.tar.gz archive, it uses a tar.gz library.
     */
    function displayConfigAndPSet(errHandler) {
        // var userWebDir = "";
        if (userWebDir === undefined || userWebDir === "") {
            errHandler(new TaskInfoUndefinedError());
            return;
        } if (sandboxUrl === "") {
            // In case proxy api returned empty or failed
            errHandler(new SandboxNotLoadedError);
            return;
        }

        var urlEnd = "/sandbox.tar.gz";

        // Goes through all the files and picks out the ones that are needed
        var tgz = TarGZ.stream(sandboxUrl + urlEnd, function(f, h) {
            if (f.filename == "debug/crabConfig.py") {
                $("#task-config-paragraph").text(f.data);
            }

            if (f.filename == "debug/originalPSet.py") {
                $("#task-pset-paragraph").text(f.data);
            }
        }, null, handleTarGZCallbackErr);
        $("#task-config-link").attr("href", userWebDir + urlEnd);
        $("#task-pset-link").attr("href", userWebDir + urlEnd);
    }

    function querySandboxApi() {
        console.log("sandbox url " + sandboxApiUrl + inputTaskName);
        $.ajax(sandboxApiUrl + inputTaskName)
            .done(function(data) {
                sandboxUrl = data.result[0];

                if (sandboxUrl === undefined || sandboxUrl == "None") {
                    sandboxUrl = "";
                }

                displayConfigAndPSet(handleConfigPSetErr);
                displayScriptExe(handleScriptExeErr);
            })
            .fail(function(xhr) {
                // TODO process error?
                console.log("error querying sandbox api");
                sandboxUrl = "";
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

        console.log("ulog link: " + url);
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
        } else if (sandboxUrl === "") {
            // In case proxy api returned empty or failed
            errHandler(new SandboxNotLoadedError);
            return;
        }


        var urlEnd = "/sandbox.tar.gz";



        var tgz = TarGZ.stream(sandboxUrl + urlEnd, function(f, h) {
            if (f.filename == scriptExe) {
                $("#script-exe-paragraph").text(f.data);
                console.log(f.data);
            }
        }, null, handleScriptExeCallbackErr);
        $("#script-exe-link").attr("href", userWebDir + urlEnd);

    }

    function displayMainPage(errHandler) {
        if (userWebDir !== "" && userWebDir !== undefined && inputTaskName !== "" && inputTaskName !== undefined) {

            var dashboardUrl = "http://dashb-cms-job.cern.ch/dashboard/templates/" + "task-analysis/#user=default&refresh=0&table=Jobs&p=1&records=25" + "&activemenu=2&status=&site=&tid=" + inputTaskName;

            var dasUrl = "https://cmsweb.cern.ch/das/request?view=list&limit=50" + "&instance=prod%2Fglobal&input=" + inputDataset;

            $("#main-dashboard-link").attr("href", dashboardUrl);

            $("#main-webdir-link").attr("href", userWebDir);
            $("#main-das-link").attr("href", dasUrl);

            var url = taskStatusUrl + inputTaskName;

            function queryApi(url) {
                $.ajax(url)
                    .done(function(data) {
                        for (var i = 0; i < data.result.length; i++) {
                            var obj = data.result[i];
                            for (var key in obj) {
                                var attrName = key;
                                var attrValue = obj[key];

                                $("#main-status-info-table tbody")
                                    .append("<tr><td>" + attrName + "</td><td>" + attrValue + "<td></tr>");
                            }
                        }
                    })
                    .fail(function(xhr) {
                        // TODO handle some errors?
                    });
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
                console.log(headerArray);
                for (var i = 0; i < headerArray.length; i++) {
                    var colonIndex = headerArray[i].search(":");
                    // console.log(headerArray[i].substr(0, colonIndex + 1) + " " + headerArray[i].substr(colonIndex + 1));
                    $("#upload-log-error-box").append("<span id=\"spaced-span\">" + headerArray[i].substr(0, colonIndex + 1) + "</span><span>" + headerArray[i].substr(colonIndex + 1) + "</span><br/>");
                }
            } else {
                $("#upload-log-error-box").css("display", "inherit").text("Couldn't load UploadLog, you can download it from the link below.");
            }

        }
    }

    function handleTaskInfoErr(err) {
        $("#task-info-error-box").empty().css("display", "inherit");

        var headers = err.headers;
        var headerArray = processErrorHeaders(headers);
        console.log(headers);
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
                $("#taskworker-log-error-box").css("display", "inherit").text("Couldn't load TaskWorker log, you can download it from the link below.");
            }
        } else if (err instanceof TaskInfoUndefinedError) {
            $("#taskworker-log-error-box").empty().css("display", "inherit").text("Task info not loaded");
        }
    }

    function handleConfigPSetErr(err) {
        if (err instanceof SandboxNotLoadedError) {
            $("#task-config-error-box").css("display", "inherit").text("Couldn't load config, please open sandbox.tar.gz " + 
                "from the link below and look for it under debug/originalConfig.py");
            $("#task-pset-error-box").css("display", "inherit").text("Couldn't load PSet, please open sandbox.tar.gz " + 
                "from the link below and look for it under debug/originalPset.py");
        } else if (err instanceof TaskInfoUndefinedError) {
            $("#task-config-error-box").css("display", "inherit").text("Task Info not loaded, can't get config");
            $("#task-pset-error-box").css("display", "inherit").text("Task Info not loaded, can't get PSet")
        }
    }

    function handleScriptExeErr(err) {
        if (err instanceof ScriptExeNotUsedError) {
            $("#script-exe-error-box").css("display", "inherit").text("ScriptExe was not used");
        } else if (err instanceof SandboxNotLoadedError) {
            $("#script-exe-error-box").css("display", "inherit").text("Couldn't load ScriptExe, please open sandbox.tar.gz " +
                "from the link below and look for " + scriptExe);
        } else if (err instanceof TaskInfoUndefinedError) {
            $("#script-exe-error-box").css("display", "inherit").text("Task info not loaded");
        }
    }

    function handleScriptExeCallbackErr(err){
        $("#script-exe-error-box").css("display", "inherit").text(err ? err : xhr.status);
    }

    function handleMainErr(err) {
        $("#main-error-box").css("display", "inherit").text("Task info not loaded");
    }

    /**
     * Callback function for handling tar file related problems. (404 not found for example)
     * Not as verbose as html headers.
     */
    function handleTarGZCallbackErr(xhr, err) {
        // Workaround for a possible bug in GZip library - if redirected 
        // to tar.gz file it throws an error but still processes it correctly
        if (err === undefined || err.search("Not a GZip file") === -1) {
            $("#task-config-error-box").css("display", "inherit").text(err ? err : xhr.status);
            $("#task-pset-error-box").css("display", "inherit").text(err ? err : xhr.status);
        }
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

    function SandboxNotLoadedError() {
        this.name = "SandboxNotLoadedError";
    }

    function setUrls(dbVersion) {
        switch (dbVersion) {
            case "prod":
                taskInfoUrl = "https://" + document.domain + "/crabserver/prod/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/prod/workflow?workflow=";
                sandboxApiUrl = "https://" + document.domain + "/crabserver/prod/task?subresource=webdirprx&workflow="
                break;
            case "preprod":
                taskInfoUrl = "https://" + document.domain + "/crabserver/preprod/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/preprod/workflow?workflow=";
                sandboxApiUrl = "https://" + document.domain + "/crabserver/preprod/task?subresource=webdirprx&workflow="
                break;
            case "dev":
                taskInfoUrl = "https://" + document.domain + "/crabserver/dev/task?subresource=search&workflow=";
                taskStatusUrl = "https://" + document.domain + "/crabserver/dev/workflow?workflow=";
                sandboxApiUrl = "https://" + document.domain + "/rabserver/dev/task?subresource=webdirprx&workflow="
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

    // Loads a task based on the name parameter the url contains.
    function processPageUrl() {
        var re = /\/task\/(.+)/;

        var result = re.exec(window.location.href);
        if (result !== undefined && result !== null) {
            inputTaskName = result[1];
            console.log(inputTaskName);

            // Set on pageload by setDefaultDnVersionSelector()
            dbVersion = $("#db-selector-box").val();
            setUrls(dbVersion);

            $("#task-search-form-input").val(inputTaskName);
            displayTaskInfo(handleTaskInfoErr);
            clearPreviousContent();
        }
    }

    function loadOtherData() {
        loadGlobalDataFromTaskInfo();
        querySandboxApi();
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

// var t = function() {
//         $.ajax("https://mmasher-mon.cern.ch/scheddmon/059/cms1425/150805_100838:erupeika_crab_tutorial_May2015_MC_analysis",
//             function(data, status, xhr) {
//                 console.log("success");
//                 console.log(data);
//                 console.log(xhr.status);

//             })
//             .fail(function(xhr) {
//                 console.log(xhr.getAllResponseHeaders());
//                 console.log(xhr.status);
//             })
//             .onerror(function() {
//                 alert("w");
//             });
//     }
    // t();