package main

import (
	"sync"
	"net/http"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tidwall/gjson"
)

type FlumeAgent struct {
	ip          string
	port        string
	name        string
	displayname string
	cluster     string
	errors      float64
	lastscrape  float64
	json        map[string]gjson.Result
}

type ClusterMember struct {
	id         string
	name       string
	ip         string
	services   []gjson.Result
	health     float64
	lastscrape float64
}

type Service struct {
	name        string
	displayname string
	servicetype string
	port        string
	cluster     string
	state       float64
	health      float64
	lastscrape  float64
}

// Exporter collects cloudera stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	URI    string
	mutex  sync.RWMutex
	client *http.Client

	exporterScrapeError *prometheus.Desc
	scrapeFailures      prometheus.Counter

	license_expiration                    *prometheus.Desc
	license_expiration_last_scrape        *prometheus.Desc
	host_health_summary                   *prometheus.Desc
	host_health_summary_last_scrape       *prometheus.Desc
	service_state                         *prometheus.Desc
	service_state_last_scrape             *prometheus.Desc
	service_health_summary                *prometheus.Desc
	service_health_summary_last_scrape    *prometheus.Desc
	flume_agent_scrape_error              *prometheus.Desc
	flume_agent_last_scrape               *prometheus.Desc
	flume_agent_event_drain_success_count *prometheus.Desc
	flume_agent_event_received_count      *prometheus.Desc
	clouderaUp                            prometheus.Gauge
}
