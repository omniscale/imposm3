package stats

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func StartHttpPProf(bind string) {
	go func() {
		log.Println(http.ListenAndServe(bind, nil))
	}()
}
