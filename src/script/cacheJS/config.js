angular.module("CRABMonitor").
    config(function($routeProvider){
        var cacheTmpUrl = "/crabserver/ui/static?html/cacheTemps/";
        var serverTmpUrl = "/crabserver/ui/static?html/serverTemps/";
        $routeProvider.
            when("/summary-view", {
                templateUrl: cacheTmpUrl+"summary-view.html",
                controller: "CacheCtrl"
        }).
            when("/task-view/:index", {
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
                templateUrl: serverTmpUrl+"userlist-view.html",
                controller: "ServerCtrl"
        });

});

