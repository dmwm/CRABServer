angular.module("CRABMonitor").
	factory("ServerUrls", function(){
		//server hostname
		var serverUrl = "";
		if(location.hostname === "cmsweb-testbed.cern.ch"){
			serverUrl = "https://"+location.hostname+"/crabserver/preprod/";
		}else if(location.hostname === "cmsweb.cern.ch"){
			serverUrl = "https://"+location.hostname+"/crabserver/prod/";
		}else{
			serverUrl = "https://"+location.hostname+"/crabserver/dev/";
		}
		var cacheUrl = "https://"+location.hostname+"/crabcache/info";

		return {
			allUsersUrl: {method: "GET", url: serverUrl+"task", params:{subresource: "allusers"}},
			taskLogsUrl: {method: "GET", url: serverUrl+"task", params:{subresource: "allusers"}},
			powerUsersUrl: {method: "GET", url: cacheUrl+"task", params: {subresource: "powerusers"}},
			quotaUrl: {method: "GET", url: cacheUrl, params: {subresource: "basicquota"}},
			usedSpacesUrl: {method: "GET", url: cacheUrl, params: {subresource: "usedspace"}},
			workflowsUrl: {method: "GET", url: serverUrl+"task", params: {subresource: "summary"}},
			searchUrl: function(task){return {method: "GET", url: serverUrl+"task", params: {subresource:"search", workflow: task}}},
			latestTaskUrl: function(date, username){
				return {method: "GET", url: serverUrl+"workflow", params: {timestamp:date, username:username}};
			}
		};
});
