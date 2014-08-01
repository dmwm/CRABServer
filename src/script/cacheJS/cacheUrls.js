angular.module("CRABMonitor").
	factory("CacheUrls", function(){
		var baseUrl = "https://"+location.hostname+"/crabcache/info";
		var logsUrl = "https://"+location.hostname+"/crabcache/logfile?name=";
		var sandboxUrl = "https://"+location.hostname+"/crabcache/file?hashkey=";
		return{
			allUserUrl: {method: "GET", url: baseUrl, params: {subresource: "listusers"}},
			usedSpacesUrl: {method: "GET", url: baseUrl, params: {subresource: "usedspace"}},
			allFilesUrl: function(name){
				return {method: "GET", url: baseUrl, params: {subresource: "userinfo",username:name,verbose:1}};
			},
			fileDetailsUrl: function(key){
				return {method: "GET", url: baseUrl, params: {subresource:"fileinfo", hashkey: key}};
			},
			powerUsersUrl: {method: "GET", url: baseUrl, params: {subresource: "powerusers"}},
			quotaUrl: {method: "GET", url: baseUrl, params: {subresource: "basicquota"}},
			logFilesUrl: function(file){
				return logsUrl+file;
			},
			sandBoxesUrl: function(file){
				return sandboxUrl+file;
			}
		};
});
