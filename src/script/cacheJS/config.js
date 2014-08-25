angular.module("CRABMonitor").
    config(function($routeProvider){
        var cacheTmpUrl = "/crabserver/ui/static?html/cacheTemps/";
        var serverTmpUrl = "/crabserver/ui/static?html/serverTemps/";
        var searchTmpUrl = "/crabserver/ui/static?html/searchTemps/";
        $routeProvider.
            when("/summary-view", {
                templateUrl: cacheTmpUrl+"summary-view.html",
                controller: "CacheCtrl"
        }).
            when("/task-view", {
                templateUrl: cacheTmpUrl+"task-view.html",
                controller: "CacheCtrl"
        }).
            when("/sandbox-view", {
                templateUrl: cacheTmpUrl+"sandbox-view.html",
                controller: "CacheCtrl"
        }).
            when("/log-view", {
                templateUrl: cacheTmpUrl+"log-view.html",
                controller: "CacheCtrl"
        }).
            when("/userlist-view", {
                templateUrl: cacheTmpUrl+"userlist-view.html",
                controller: "CacheCtrl"
        }).//for crab server routing
            when("/workflows", {
                templateUrl: serverTmpUrl+"workflows-view.html",
                controller: "ServerCtrl"
        }).
            when("/latest-tasks", {
                templateUrl: serverTmpUrl+"latest-task.html",
                controller: "ServerCtrl"
        }).
            when("/workflow", {
                templateUrl: serverTmpUrl+"user-workflow-view.html",
                controller: "SearchCtrl"
        }).//for quick search routing
            when("/taskinfo", {
                templateUrl: searchTmpUrl+"task-info-view.html",
                controller: "SearchCtrl"
        }).
            when("/jobinfo", {
                templateUrl: searchTmpUrl+"job-info.html",
                controller: "SearchCtrl"
		
        }).
            when("/pubinfo", {
                templateUrl: searchTmpUrl+"pub-info.html",
                controller: "SearchCtrl"
        }).
            when("/tasklinks", {
                templateUrl: searchTmpUrl+"task-links.html",
                controller: "SearchCtrl"
		}).
            when("/job/:id/info", {
                templateUrl: searchTmpUrl+"selected-job-info.html",
                controller: "SearchCtrl"
		}).
            when("/available-sites/:id", {
                templateUrl: searchTmpUrl+"available-sites.html",
                controller: "SearchCtrl"
		}).
			when("/", {
				templateUrl: serverTmpUrl+"welcome.html",
				controller: "ServerCtrl"
		})
	});
