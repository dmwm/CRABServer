angular.module("CRABMonitor").
	factory("ServerUrls", function(){
		//server hostname
		var baseUrl = "https://"+location.hostname;
		return {
			allUsersUrl: {method: "GET", url: baseUrl+"/crabserver/dev/task", params:{subresource: "allusers"}}
		};
});
