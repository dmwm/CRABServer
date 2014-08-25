/*
	This file contains just one line of code. It is the definition of the angular
	module.  The controllers and the config file are injected into it during 
	runtime. You can inject any angularJS module that would be required for operation
	in the array in this definition.  Currently only the ngRoute module is being
	injected for the purpose of ROUTING!  Make sure in the index.html this script
	is injected first before all other scripts that depends on it
*/
angular.module("CRABMonitor", ["ngRoute"]);
