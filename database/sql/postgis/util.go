package postgis

import (
	"os"
	"strings"
)

// disableDefaultSslOnLocalhost adds sslmode=disable to params
// when host is localhost/127.0.0.1 and the sslmode param and
// PGSSLMODE environment are both not set.
func disableDefaultSslOnLocalhost(params string) string {
	parts := strings.Fields(params)
	isLocalHost := false
	for _, p := range parts {
		if strings.HasPrefix(p, "sslmode=") {
			return params
		}
		if p == "host=localhost" || p == "host=127.0.0.1" {
			isLocalHost = true
		}
	}

	if !isLocalHost {
		return params
	}

	for _, v := range os.Environ() {
		parts := strings.SplitN(v, "=", 2)
		if parts[0] == "PGSSLMODE" {
			return params
		}
	}

	// found localhost but explicit no sslmode, disable sslmode
	return params + " sslmode=disable"
}

func prefixFromConnectionParams(params string) string {
	parts := strings.Fields(params)
	var prefix string
	for _, p := range parts {
		if strings.HasPrefix(p, "prefix=") {
			prefix = strings.Replace(p, "prefix=", "", 1)
			break
		}
	}
	if prefix == "" {
		prefix = "osm_"
	}
	if prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}
	return prefix
}
