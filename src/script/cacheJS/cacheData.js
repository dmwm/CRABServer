angular.module("CRABMonitor").
	factory("CacheData", function(){
	return {users: [], basicQuota: 0, powerUsers: [],userSummary: [],
		selectedUser: {
		username: "",
		index: 0,
		sandBox: [],
		isPowerUser: false,
		logs: [],
		numOfSanboxes: 0,
		numOfLogs: 0,
		storage:{usedSpace: 0, isRed: false, isGreen: true, percentUsed: 0.0}
	}};
});
