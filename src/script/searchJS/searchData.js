angular.module("CRABMonitor").
	factory("SearchData", function(){
		return {
			taskname: "",
			user: {name: "", href: ""},
			sandbox: {file: "", href:""},
			data: {},
			pubInfo:{},
			jobStatus:{},
			jobFiles:[], //array of objects {filename,size,lfn,pfn}
			taskLinks:{
				glidein: "",
				dashboard: "",
				aso: "",
				das: ""
			},
			numOfJobs:0,
			jobNum: 0,
			"Available sites":[],
			selectedJobInfo:{
				Retries: "",
				"Start time":0,
				"Submit time":0,
				"Job ID":"",
				Restarts: 0,
				"Recorded site": "",
				State: "",
				"Wall Duration": "",
				"End Time": "",
				"Total system CPU time history":0,
				"Resident set size":0,
				"Total user CPU time history":0,
				"Site history": ""
			}
		};
});
