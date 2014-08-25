angular.module("CRABMonitor").
	factory("SearchFuncs", ["SearchData", "ServerUrls","$http", function(SearchData, ServerUrls, $http){
	//set the name of the task to search
	var settask = function(name){
		SearchData.taskname = name;	
	};
	//count the number of jobs in workflow and use it to restrict job number input
	var countJobs = function(workflow){
		$http(ServerUrls.task0Url(workflow)).then(function(response){
			for(key in response.data.result[0].jobs){
				SearchData.numOfJobs++;
			}
			//load the publication info
			for(var i in response.data.result[0].publication){
				SearchData.pubInfo[i] = response.data.result[0].publication[i];
			}
			//load job status
			for(var i in response.data.result[0].jobsPerStatus){
				SearchData.jobStatus[i] = response.data.result[0].jobsPerStatus[i];
			}
		}, function(response){
			console.log("Request failed with status: "+response.status);
		});
	};
	//load a selected job info with verbose 1 & 2
	
	var selectedJobInfo = function(){
		//load verbose 1
		$http(ServerUrls.verboseUrl(SearchData.taskname, 1)).then(function(response){
			SearchData.selectedJobInfo.Retries = response.data.result[0].jobs[SearchData.jobNum].Retries;
			SearchData.selectedJobInfo["Start time"] = new Date(response.data.result[0].jobs[SearchData.jobNum].StartTimes*1000);
			SearchData.selectedJobInfo["Submit time"] = new Date(response.data.result[0].jobs[SearchData.jobNum].SubmitTimes*1000);
			SearchData.selectedJobInfo["Job ID"] = response.data.result[0].jobs[SearchData.jobNum].JobIds;
			SearchData.selectedJobInfo.Restarts = response.data.result[0].jobs[SearchData.jobNum].Restarts;
			SearchData.selectedJobInfo["Recorded site"] = response.data.result[0].jobs[SearchData.jobNum].RecordedSite;
			SearchData.selectedJobInfo.State = response.data.result[0].jobs[SearchData.jobNum].State;
			SearchData.selectedJobInfo["Wall Duration"] = response.data.result[0].jobs[SearchData.jobNum].WallDurations+" s ";
			SearchData.selectedJobInfo["End Time"] = new Date(response.data.result[0].jobs[SearchData.jobNum].EndTimes*1000);
			SearchData.selectedJobInfo["Total system CPU time history"] = response.data.result[0].jobs[SearchData.jobNum].TotalSysCpuTimeHistory[0];
			SearchData.selectedJobInfo["Resident set size"] = response.data.result[0].jobs[SearchData.jobNum].ResidentSetSize+" Bytes ";
			SearchData.selectedJobInfo["Total user CPU time history"] = response.data.result[0].jobs[SearchData.jobNum].TotalUserCpuTimeHistory[0];
			SearchData.selectedJobInfo["Site history"] = response.data.result[0].jobs[SearchData.jobNum].SiteHistory;
		}, function(response){
			console.log("Request with verbose 1 failed with status: "+response.status);
		});

		//load with verbose 2
		$http(ServerUrls.verboseUrl(SearchData.taskname, 2)).then(function(response){
			for(var i = 0; i < response.data.result[0].jobs[SearchData.jobNum].AvailableSites.length; i++){
				SearchData["Available sites"].push(response.data.result[0].jobs[SearchData.jobNum].AvailableSites[i]);
			}
		}, function(response){
			console.log("Request with verbose 2 failed with status: "+response.status);
		});
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
			setTask: settask,
			search: search,
			glidein: glidein,
			dashboard:dashboard,
			getNumOfJobs:countJobs,
			selectedJobInfo: selectedJobInfo
	};
}]);
