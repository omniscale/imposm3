package cmd

var Version string

func init() {
	// buidVersion gets replaced during build
	var BuildVersion = ""
	Version = "0.1"
	Version += BuildVersion
}
