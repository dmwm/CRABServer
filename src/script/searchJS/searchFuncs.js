angular.module("CRABMonitor").
	factory("SearchFuncs", ["SearchData", "ServerUrls","$http", function(SearchData, ServerUrls, $http){
	//set the name of the task to search
	var settask = function(name){
		SearchData.taskname = name;	
	};

	//query the task from the database
	var search = function(){
		var tmpurl = ["/file?hashkey="];
		$http(ServerUrls.searchUrl(SearchData.taskname)).then(function(response){
			if(response.data.result.length > 0){
				for( var i = 0; i < response.data.desc.columns.length; i++){
					if(response.data.result[0][i] === "[]"){
						SearchData.data[response.data.desc.columns[i].slice(3)] = "[ empty ]"; 
					}else{
						if(response.data.desc.columns[i].slice(3) === "username"){
							SearchData.user.name = response.data.result[0][i];
							SearchData.user.href = "#/latest-tasks";
						}
						else if(response.data.desc.columns[i].slice(3) === "user_sandbox"){
							tmpurl.push(response.data.result[0][i].split("\.")[0]);
							SearchData.sandbox.file = response.data.result[0][i].split("\.")[0];
						}
						else if(response.data.desc.columns[i].slice(3) === "cache_url"){
							tmpurl.unshift(response.data.result[0][i]);
						}
						else{
							SearchData.data[response.data.desc.columns[i].slice(3)] = response.data.result[0][i];	
						}
					}
				}
				SearchData.sandbox.href = tmpurl.join("");
			}
		}, function(response){
			console.log("Request for "+SearchData.taskname+" failed with status: "+response.status);
		});
	};
	//task links
   var glidein = function(){
		return "http://glidemon.web.cern.ch/glidemon/jobs.php?taskname="+SearchData.taskname;
	};

   var dashboard = function(){
        return " http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#user="+ SearchData.user.name+"&refresh=0&table=Jobs&p=1&records=25&activemenu=2&status=&site=&tid="+SearchData.taskname
    };

	//job info loader
	var jobFiles = function(){
		
	};

	return {
		setTask: settask, search: search, glidein: glidein, dashboard:dashboard
	};
}]);
