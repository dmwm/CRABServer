angular.module("CRABMonitor").
	factory("ServerData", function(){
			return {
				users: [],
				basicQuota: 0,
				workflows: {},
				selectedUser:{
					username: ""
				}
			};
});
