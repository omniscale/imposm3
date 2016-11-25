package imposm3

var Version string

func init() {
	// buidVersion gets replaced during build with make
	var buildVersion = ""
	Version = "0.2.0"
	Version += buildVersion
}
