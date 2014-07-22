angular.module("CRABMonitor").
	controller("CacheCtrl", ["$scope", "CacheData", "CacheFuncs",function($scope,CacheData,CacheFuncs){
		//bind the data services to the scope
		$scope.data = CacheData;
		$scope.funcs = CacheFuncs;
		//loadUsers: request users if none or nothing
		$scope.loadUsers = function(){
			if($scope.data.users.length === 0){
				$scope.funcs.getAllUsers();
				$scope.funcs.getPowerUsers();
				$scope.funcs.getQuota();
			}
		};

		//load the user summary when btn clicked: needs new API to be implemented
		$scope.listAllUsersSummary = function(){
			//API not implemented
		};

		//select a user
		$scope.selectUser = function(index){
			if($scope.data.selectedUser.sandBox.length > 0){
				$scope.funcs.resetUser();
			}
			$scope.data.index = index;
			$scope.funcs.selectUser($scope.data.users[index]);
		};
		$scope.logLink = function(name){
				return $scope.funcs.logFileLink(name);
		};

}]);
