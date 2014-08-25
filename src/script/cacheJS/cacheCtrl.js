/*
	This is the controller for the CRAB Cache panel.  Every activity performed
	on the CRABCache panel is coordinated here!. The functions, Data, and Urls
	are all defined in the cacheData,js, cacheFuncs.js, cacheUrls.js and are
	injected into this controller at runtime.  The functions defined in this script
	file are functions that are directly called from the html view.
*/
angular.module("CRABMonitor").
	controller("CacheCtrl", ["$scope", "CacheData", "CacheFuncs","$location",function($scope,CacheData,CacheFuncs,$location){
		//bind the data services to the scope
		$scope.cacheData = CacheData;
		$scope.cacheFuncs = CacheFuncs;
		//loadUsers: request users if none or nothing
		$scope.loadCacheUsers = function(event, name){
			if(event.keyCode === 13 && $scope.cacheData.users.length > 0){
				$scope.cacheFuncs.selectUser(name);
				$location.path("/task-view/");
			}
			else{
			
			 if($scope.cacheData.users.length === 0){
					$scope.cacheFuncs.getAllUsers();
					$scope.cacheFuncs.getPowerUsers();
					$scope.cacheFuncs.getQuota();
				}
			}
		};
		
		//LIST A SUMMARY OF ALL USERS INFO
        $scope.userList = function(){
            if($scope.cacheData.users.length === 0){ 
                $scope.cacheFuncs.getAllUsers();
                $scope.cacheFuncs.getQuota();
            }
            $scope.cacheFuncs.getSummary();
            $location.path("/userlist-view");
            console.log($scope.cacheData);
        };

		//select a user
		$scope.selectUser = function(name, index){
			if($scope.cacheData.selectedUser.sandBox.length > 0){
				$scope.cacheFuncs.resetUser();
			}
			$scope.selectedRow = index;
			//$scope.cacheData.index = index;
			$scope.cacheFuncs.selectUser(name);
		};

		$scope.logLink = function(name){
				return $scope.cacheFuncs.logFileLink(name);
		};
		
		//select user with name
		$scope.selectUserWithName = function(name){
			if($scope.cacheData.selectedUser.sandBox.length > 0){
				$scope.cacheFuncs.resetUser();
			}	
			$scope.cacheFuncs.selectUser(name);
		};
}]);
