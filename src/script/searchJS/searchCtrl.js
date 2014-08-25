angular.module("CRABMonitor").
	controller("SearchCtrl", ["$scope","SearchFuncs", "SearchData","ServerData","ServerFuncs", "$location", function($scope, SearchFuncs, SearchData,ServerData,ServerFuncs,$location){
		//bind the scope to the factories
		$scope.searchData = SearchData;
		$scope.searchFuncs = SearchFuncs;
		$scope.serverData = ServerData; 
		$scope.serverFuncs = ServerFuncs;

		//what to do when enter is pressed
		$scope.taskSearch = function(taskname, event){
			if(event !== undefined && event.keyCode === 13 && taskname !== ""){
				$scope.searchFuncs.setTask(taskname);
				$scope.searchFuncs.search();
				$location.path("/workflow");
				$scope.searchFuncs.getNumOfJobs(taskname);
			}else if(event === undefined && taskname  !== ""){
				$scope.searchFuncs.setTask(taskname);
				$scope.searchFuncs.search();
				$location.path("/workflow");
				$scope.searchFuncs.getNumOfJobs(taskname);
			}
		};

		//when the username is clicked'
		$scope.checkUserTasks = function(name){
			$scope.serverData.selectedUser.username = name;
			$scope.serverFuncs.getLatests();
		};

		//tasklinks clicked
		$scope.getLinks = function(){
			$scope.searchData.taskLinks.glidein = $scope.searchFuncs.glidein();
			$scope.searchData.taskLinks.dashboard = $scope.searchFuncs.dashboard();
		};
		//what happens when get info about job # Go btn is clicked
		$scope.loadJobInfo = function(number){
			$scope.searchData.jobNum = number;
			$scope.searchFuncs.selectedJobInfo();
			$location.path("/job/"+number+"/info");
		};


}]);
