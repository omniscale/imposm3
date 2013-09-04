package main

var imposmVersion string

func init() {
	// buidVersion gets replaced during build
	var buildVersion = ""
	imposmVersion = "0.1"
	imposmVersion += buildVersion
}
