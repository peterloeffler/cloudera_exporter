package main

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "cloudera"
)

// command line parameter variables
var (
	listeningAddress            = kingpin.Flag("telemetry.address", "Address on which to expose metrics.").Default(":9512").String()
	metricsEndpoint             = kingpin.Flag("telemetry.endpoint", "Path under which to expose metrics.").Default("/metrics").String()
	httpClientInsecure          = kingpin.Flag("http.client.insecure", "Ignore server certificate if using https").Default("true").Bool()
	httpClientTimeout           = kingpin.Flag("http.client.timeout", "Timeout for the go http client in seconds for the request to the Cloudera API and the Flume Agents.").Default("10").Int()
	clouderaApiUri              = kingpin.Flag("cloudera.api.uri", "Cloudera API URI").Default("http://localhost:7180/api/v6").String()
	clouderaApiScrapeInterval   = kingpin.Flag("cloudera.api.scrape.interval", "Interval to scrape the Cloudera API in seconds for scraping in the background (don't forget to also set -cloudera.flume.scrape.interval).").Default("0").Int()
	clouderaFlumeScrapeInterval = kingpin.Flag("cloudera.flume.scrape.interval", "Interval to scrape the Cloudera Flume Agents in seconds for scraping in the background (don't forget to also set -clouderaApiScrapeInterval).").Default("0").Int()
)

// exporter specific variables
var (
	flumeLabelNames                                       = []string{"ipaddress", "port", "name", "displayname", "cluster", "process"}
	flumeErrorLabelNames                                  = []string{"ipaddress", "port", "name", "displayname", "cluster"}
	hostLabelNames                                        = []string{"hostid", "name", "ipaddress"}
	serviceLabelNames                                     = []string{"name", "displayname", "cluster"}
	clouderauser                                          string
	clouderapass                                          string
	scrapeError                                           float64
	jobGetConfigJsonInProgress, jobGetFlumeJsonInProgress bool
)

// global variables for parallel data collecting
var (
	globalLicenseExpiration           int64
	globalLicenseExpirationLastScrape float64
	globalFlumeMetrics                []FlumeAgent
	globalClusterMemberMetrics        []ClusterMember
	globalServiceMetrics              []Service
)
