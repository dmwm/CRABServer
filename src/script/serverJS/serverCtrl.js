angular.module("CRABMonitor").
	controller("ServerCtrl", ["$scope","ServerData","ServerFuncs","SearchFuncs","$location", function($scope, ServerData,ServerFuncs,SearchFuncs, $location){
		//INJECT THE FACTORIES INTO THE CONTROLLER
		$scope.serverData = ServerData;
		$scope.serverFuncs = ServerFuncs;
		$scope.searchFuncs = SearchFuncs;

		//LOAD THE USERS IN CRABSERVER THE OPERATOR BEGINS TO TYPE
		$scope.loadUsers = function(name, event){
			if(event.keyCode === 13 && $scope.serverData.users.length > 0){
				//select a user
				$scope.serverData.selectedUser.username = name;
				$location.path("/latest-tasks");
			}else{
				if($scope.serverData.users.length === 0){
					$scope.serverFuncs.getUsers();
					$scope.serverFuncs.getQuota();
				}
			}
		};

		//when a task in clicked in a server's display
		$scope.followTaskLink = function(taskname){
			$scope.searchFuncs.setTask(taskname);
			$scope.searchFuncs.search();
			$location.path("/workflow");
			$scope.searchFuncs.getNumOfJobs(taskname);
		};	
		//LIST A SUMMARY OF ALL USERS INFO
		$scope.userList = function(){
			$scope.serverFuncs.getWorkflows();
			$location.path("/workflows");
		};

		//select a user
		$scope.selectUser = function(name, index){
			$scope.selectedRow = index;
			$scope.serverData.selectedUser.username = name;
			$scope.serverFuncs.getLatests();
		};
		//load latest user tasks
		$scope.loadLatestTasks = function(date){
			$scope.serverFuncs.getLatests(date);
		};

}]);
