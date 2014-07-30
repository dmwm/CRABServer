angular.module("CRABMonitor").
	controller("ServerCtrl", ["$scope","ServerData","ServerFuncs","$location", function($scope, ServerData,ServerFuncs, $location){
		//INJECT THE FACTORIES INTO THE CONTROLLER
		$scope.serverData = ServerData;
		$scope.serverFuncs = ServerFuncs;

		//LOAD THE USERS IN CRABSERVER THE OPERATOR BEGINS TO TYPE
		$scope.loadUsers = function(name, event){
			if(event.keyCode === 13 && $scope.serverData.users.length > 0){
				//select a user
				$scope.serverData.selectedUser.username = name;
				$location.path("/user-workflow-view");
			}else{
				if($scope.serverData.users.length === 0){
					$scope.serverFuncs.getUsers();
					$scope.serverFuncs.getQuota();
				}
			}
		};


		//LIST A SUMMARY OF ALL USERS INFO
		$scope.userList = function(){
			$scope.serverFuncs.getWorkflows();
			$location.path("/workflows-view");
		};

		//select a user
		$scope.selectUser = function(name, index){
			$scope.selectedRow = index;
			$scope.serverData.selectedUser.username = name;
		}
}]);
