angular.module("CRABMonitor").
	factory("ServerUrls", function(){
		//server hostname
		var serverUrl = "";
		if(location.hostname === "cmsweb-testbed.cern.ch"){
			serverUrl = "https://"+location.hostname+"/crabserver/preprod/task";
		}else if(location.hostname === "cmsweb.cern.ch"){
			serverUrl = "https://"+location.hostname+"/crabserver/prod/task";
		}else{
			serverUrl = "https://"+location.hostname+"/crabserver/dev/task";
		}
		var cacheUrl = "https://"+location.hostname+"/crabcache/info";

		return {
			allUsersUrl: {method: "GET", url: serverUrl, params:{subresource: "allusers"}},
			taskLogsUrl: {method: "GET", url: serverUrl, params:{subresource: "allusers"}},
			powerUsersUrl: {method: "GET", url: cacheUrl, params: {subresource: "powerusers"}},
			quotaUrl: {method: "GET", url: cacheUrl, params: {subresource: "basicquota"}},
			usedSpacesUrl: {method: "GET", url: cacheUrl, params: {subresource: "usedspace"}},
			workflowsUrl: {method: "GET", url: serverUrl, params: {subresource: "summary"}},
			searchUrl: function(task){return {method: "GET", url: serverUrl, params: {subresource:"search", workflow: task}}},
		};
});
