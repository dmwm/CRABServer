angular.module("CRABMonitor").
	controller("ServerCtrl", ["$scope","ServerData","ServerFuncs","$location", function($scope, ServerData,ServerFuncs, $location){
		//INJECT THE FACTORIES INTO THE CONTROLLER
		$scope.data = ServerData;
		$scope.funcs = ServerFuncs;

		//LOAD THE USERS IN CRABSERVER THE OPERATOR BEGINS TO TYPE
		$scope.loadUsers = function(){
			if($scope.data.users.length === 0){
				$scope.funcs.getUsers();				
			}
		};

		//LIST A SUMMARY OF ALL USERS INFO
		$scope.userList = function(){
			$location.path("/userlist-view");
		};
}]);
