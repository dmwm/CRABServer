angular.module("CRABMonitor").
	factory("ServerFuncs", ["ServerData", "ServerUrls","$http",function(ServerData, ServerUrls, $http){
		//LOAD ALL USERS
		var users = function(){
			$http(ServerUrls.allUsersUrl).then(function(response){
				for(var i = 0; i < response.data.result.length; i++){
					ServerData.users.push(response.data.result[i][0]);
				}
			}, function(response){
				console.log("Request for users  failed with status: "+response.status);
			});
		};
		return {getUsers: users};
}]);
