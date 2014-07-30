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
            when("/workflows-view", {
                templateUrl: serverTmpUrl+"workflows-view.html",
                controller: "ServerCtrl"
        }).
            when("/user-workflow-view", {
                templateUrl: serverTmpUrl+"user-workflow-view.html",
                controller: "ServerCtrl"
        }).//for quick search routing
            when("/search-view", {
                templateUrl: searchTmpUrl+"search-view.html",
                controller: "SearchCtrl"
		})
	});
