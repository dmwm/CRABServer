angular.module("CRABMonitor").
	factory("SearchData", function(){
		return {
			taskname: "",
			user: {name: "", href: ""},
			sandbox: {file: "", href:""},
			data: {},
			jobFiles:[], //array of objects {filename,size,lfn,pfn}
			taskLinks:{
				glidein: "",
				dashboard: "",
				aso: "",
				das: ""
			}
		};
});
