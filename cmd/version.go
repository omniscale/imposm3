package cmd

var Version string

func init() {
	// buidVersion gets replaced during build with make
	var buildVersion = ""
	Version = "0.1"
	Version += buildVersion
}
