angular.module("CRABMonitor").
	controller("SearchCtrl", ["$scope","SearchFuncs", "SearchData","ServerData", "$location", function($scope, SearchFuncs, SearchData,ServerData,$location){
		//bind the scope to the factories
		$scope.searchData = SearchData;
		$scope.searchFuncs = SearchFuncs;
		$scope.serverData = ServerData; 

		//what to do when enter is pressed
		$scope.taskSearch = function(taskname, event){
			if(event !== undefined && event.keyCode === 13 && taskname !== ""){
				$scope.searchFuncs.setTask(taskname);
				$scope.searchFuncs.search();
				$location.path("/workflow");
			}else if(event === undefined && taskname  !== ""){
				$scope.searchFuncs.setTask(taskname);
				$scope.searchFuncs.search();
				$location.path("/workflow");
			}
		};

		//when the username is clicked'
		$scope.checkUserTasks = function(name){
			$scope.serverData.selectedUser.username = name;
		};

		//tasklinks clicked
		$scope.getLinks = function(){
			$scope.searchData.taskLinks.glidein = $scope.searchFuncs.glidein();
			$scope.searchData.taskLinks.dashboard = $scope.searchFuncs.dashboard();
		};

}]);
