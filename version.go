package imposm3

var Version string

// buidVersion gets replaced while building with
// go build -ldflags "-X github.com/omniscale/imposm3.buildVersion 1234"
var buildVersion string

func init() {
	Version = "0.3.0"
	Version += buildVersion
}
