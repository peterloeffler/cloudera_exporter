package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"gopkg.in/alecthomas/kingpin.v2"
	"net/http"
	"os"
)

func main() {
	log.AddFlags(kingpin.CommandLine)
	kingpin.Parse()

	clouderauser = os.Getenv("CLOUDERAUSER")
	clouderapass = os.Getenv("CLOUDERAPASS")

	if clouderauser == "" || clouderapass == "" {
		log.Fatal("Please set Cloudera username and password!")
	}

	// do initial load of config from Cloudera API
	if *clouderaApiScrapeInterval > 0 {
		log.Infof("Execute initial config load from Cloudera API...")
		getConfigJson()
	}
	if *clouderaFlumeScrapeInterval > 0 {
		log.Infof("Execute initial parsing of all Flume Agents...")
		getFlumeJson()
	}

	exporter := ClouderaExporter(*clouderaApiUri)
	prometheus.MustRegister(exporter)

	log.Infof("Starting Server...")
	log.Infof("Listening on %s", *listeningAddress)
	log.Infof("Cloudera URI: %s", *clouderaApiUri)
	http.Handle(*metricsEndpoint, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Cloudera Exporter</title></head>
			<body>
			<h1>Cloudera Exporter</h1>
			<p><a href="` + *metricsEndpoint + `">Metrics</a></p>
			</body>
			</html>`))
	})

	// Collect everything regularly in the background
	if *clouderaApiScrapeInterval > 0 {
		go func() {
			jobGetConfigJson()
		}()
	}
	if *clouderaFlumeScrapeInterval > 0 {
		go func() {
			jobGetFlumeJson()
		}()
	}

	log.Fatal(http.ListenAndServe(*listeningAddress, nil))
}
