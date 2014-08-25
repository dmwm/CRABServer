/*
	This factory is injected into the the main module during runtime
	It is responsible for the creation of the URLs used in the CRAB
	server panel. URLs that require specific parameters are functions
	that creates and return the URL.
*/
angular.module("CRABMonitor").
	factory("ServerUrls", function(){
		//creation of host name based on the server on which it is deployed
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
			//task0Url is the url for verbose = 0
			task0Url: function(workflow){ 
				return {method: "GET", url: serverUrl+"workflow", params: {workflow:workflow}}
			},
			//verboseUrl is the url for verbose = 1 and 2
			verboseUrl: function(workflow, value){ 
				return {method: "GET", url: serverUrl+"workflow", params: {workflow:workflow, verbose:value}};		
			}
			,
			searchUrl: function(task){
				return {method: "GET", url: serverUrl+"task", params: {subresource:"search", workflow: task}}
			},
			latestTaskUrl: function(date, username){
				return {method: "GET", url: serverUrl+"workflow", params: {timestamp:date, username:username}};
			}
		};
});
