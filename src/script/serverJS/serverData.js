angular.module("CRABMonitor").
	factory("ServerData", function(){
			return {
				users: [],
				basicQuota: 0,
				workflows: {},
				selectedUser:{
					username: ""
				},
				latestData: []	//array of obj [{taskname, status,tw_name, spliti_args}]
			};
});
