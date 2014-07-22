angular.module("CRABMonitor").
	factory("CacheFuncs", ["$http", "CacheData", "CacheUrls",function($http, CacheData, CacheUrls){
	//request Users
	var reqUsers = function(){
		$http(CacheUrls.allUserUrl).then(function(response){
			for(var i = 0; i < response.data.result.length; i++){
				CacheData.users.push(response.data.result[i]);
			}
		}, function(response){
				console.log("Request for all users failed with a status: "+ response.status);
		});
	};
	//request power users
	var powerUsers = function(){
		$http(CacheUrls.powerUsersUrl).then(function(response){
			for(var i = 0; i < response.data.result.length; i++){
				CacheData.powerUsers.push(response.data.result[i]);
			}
		}, function(response){
			console.log("Request for power users failed with status: "+ response.status);
		});
	};
	//create a downloadable link
	var logFileLink = function(file){
		return CacheUrls.logFilesUrl(file);
	};
	var sandBoxLink = function(sandbox){
		return CacheUrls.sandBoxesUrl(sandbox);
	};

	//request basic quota
	var quota = function(){
		$http(CacheUrls.quotaUrl).then(function(response){
			CacheData.basicQuota = response.data.result[0].quota_user_limit*1000;	
		}, function(response){
			console.log("Request for basic quota failed with status: "+response.status);
		});
	};	
	
	//get user file details
	var userFiles = function(){
		$http(CacheUrls.allFilesUrl(CacheData.selectedUser.username)).then(function(response){
			for(var key in response.data.result[0].file_list){
				if(key.split("\.")[1] === "log"){
					CacheData.selectedUser.logs.push(response.data.result[0].file_list[key][0]);
					CacheData.selectedUser.numOfLogs++;
				}else{
					CacheData.selectedUser.sandBox.push(response.data.result[0].file_list[key][0]);
					CacheData.selectedUser.numOfSanboxes++;
				}
			}
			CacheData.selectedUser.storage.usedSpace = response.data.result[0].used_space[0];
			if(CacheData.selectedUser.isPowerUser){
				CacheData.selectedUser.storage.percentUsed = CacheData.selectedUser.storage.usedSpace / CacheData.basicQuota * 10;
			}else{
				CacheData.selectedUser.storage.percentUsed = CacheData.selectedUser.storage.usedSpace / CacheData.basicQuota * 100;
			}
			console.log(CacheData.selectedUser.storage.usedSpace);
		}, function(response){
			console.log("Request for files details failed with status: "+response.status);
		});
	};

	//reset selectedUser
	var reset = function(){
		CacheData.selectedUser.username = "";
		while(CacheData.selectedUser.sandBox.length > 0){
			CacheData.selectedUser.sandBox.pop();
			if(CacheData.selectedUser.logs.length > 0){
				CacheData.selectedUser.logs.pop();
			}
		}
		CacheData.selectedUser.isPowerUser = false;
		CacheData.selectedUser.numOfSanboxes = 0;
		CacheData.selectedUser.numOfLogs = 0;
		CacheData.selectedUser.storage.usedSpace = 0;
		CacheData.selectedUser.storage.isRed = false;
		CacheData.selectedUser.storage.isGreen = true;
		CacheData.selectedUser.storage.percentUsed = 0
	};

var selectUser = function(name) {
    CacheData.selectedUser.username = name;
    //check whether is a power user
    for (var i = 0; i < CacheData.powerUsers.length; i++) {
        if (CacheData.powerUsers[i] === name) {
            CacheData.selectedUser.isPowerUser = true;
            break;
        }
    }
    console.log(CacheData.selectedUser.isPowerUser);
    console.log(CacheData.basicQuota);

	//set sandboxes, logs and their numbers
	userFiles();
    console.log("usedspace: "+ CacheData.selectedUser.storage.usedSpace);
    console.log("division: "+ (CacheData.selectedUser.storage.usedSpace/CacheData.basicQuota));
    if (CacheData.selectedUser.storage.percentUsed > 80.0) {
        CacheData.selectedUser.storage.isRed = true;
        CacheData.selectedUser.storage.isGreen = false;
    } else {
        CacheData.selectedUser.storage.isRed = false;
        CacheData.selectedUser.storage.isGreen = true;
    }
    console.log(CacheData.selectedUser.storage.percentUsed);
};

	return {
			getAllUsers: reqUsers,
			getPowerUsers: powerUsers, 
			getQuota: quota, 
			selectUser: selectUser,
			resetUser: reset,
			getUserFiles: userFiles,
			sandBoxlink: sandBoxLink(name),
			logFileLink: logFileLink(name)
	};
}]);
