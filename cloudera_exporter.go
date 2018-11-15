package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	namespace = "cloudera"
)

// command line parameter variables
var (
	listeningAddress            = flag.String("telemetry.address", ":9512", "Address on which to expose metrics.")
	metricsEndpoint             = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
	insecure                    = flag.Bool("insecure", true, "Ignore server certificate if using https")
	httpClientTimeout           = flag.Int("http.client.timeout", 10, "Timeout for the go http client in seconds for the request to the Cloudera API and the Flume Agents.")
	clouderaApiUri              = flag.String("cloudera.api.uri", "http://localhost:7180/api/v6", "Cloudera API URI")
	clouderaApiScrapeInterval   = flag.Int("cloudera.api.scrape.interval", 300, "Interval to scrape the Cloudera API in seconds.")
	clouderaFlumeScrapeInterval = flag.Int("cloudera.flume.scrape.interval", 60, "Interval to scrape the Cloudera Flume Agents in seconds.")
)

// exporter specific variables
var (
	flumeLabelNames      = []string{"ipaddress", "port", "name", "displayname", "cluster", "process"}
	flumeErrorLabelNames = []string{"ipaddress", "port", "name", "displayname", "cluster"}
	hostLabelNames       = []string{"hostid", "name", "ipaddress"}
	serviceLabelNames    = []string{"name", "displayname", "cluster"}
	clouderauser         string
	clouderapass         string
	scrapeError          float64
)

// global variables for parallel data collecting
var (
	globalHosts, globalServices                           string
	globalHostsDetail, globalServicesDetail               []string
	globalLicenseExpiration                               int64
	jobGetConfigJsonInProgress, jobGetFlumeJsonInProgress bool
	flumeHosts                                            []string
	flumeRoleRefs                                         []gjson.Result
	flumeMetrics                                          []FlumeAgent
)

type FlumeAgent struct {
	ip          string
	port        string
	name        string
	displayname string
	cluster     string
	errors      float64
	json        map[string]gjson.Result
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
	host_health_summary                   *prometheus.Desc
	service_state                         *prometheus.Desc
	service_health_summary                *prometheus.Desc
	flume_agent_scrape_error              *prometheus.Desc
	flume_agent_event_drain_success_count *prometheus.Desc
	flume_agent_event_received_count      *prometheus.Desc
	clouderaUp                            prometheus.Gauge
}

// ClouderaExporter returns an initialized Exporter.
func ClouderaExporter(uri string) *Exporter {
	return &Exporter{
		URI: uri,
		exporterScrapeError: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "exporter", "last_scrape_error"),
			"Whether the last scrape of metrics resulted in an error (1 for error, 0 for success).",
			nil, nil,
		),
		scrapeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrape_failures_total",
			Help:      "Number of errors while scraping cloudera.",
		}),
		license_expiration: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "license_expiration"),
			"Cloudera License Expiration.",
			nil, nil,
		),
		host_health_summary: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "host_health_summary"),
			"Cloudera Host Health Summary. 0 is GOOD, 1 is something else.",
			hostLabelNames, nil,
		),
		service_state: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "service_state"),
			"Cloudera Service State. 0 is STARTED, 1 is something else.",
			serviceLabelNames, nil,
		),
		service_health_summary: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "service_health_summary"),
			"Cloudera Service Health Summary. 0 is GOOD, 1 is something else.",
			serviceLabelNames, nil,
		),
		flume_agent_scrape_error: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "flume_agent_scrape_error"),
			"Cloudera Flume Agent Scrape Error. 0 is OK.",
			flumeErrorLabelNames, nil,
		),
		flume_agent_event_drain_success_count: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "flume_agent_event_drain_success_count"),
			"Cloudera Flume Agent EventDrainSuccessCount.",
			flumeLabelNames, nil,
		),
		flume_agent_event_received_count: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "flume_agent_event_received_count"),
			"Cloudera Flume Agent EventReceivedCount.",
			flumeLabelNames, nil,
		),
		clouderaUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the cloudera is up.",
		}),
		client: &http.Client{
			Timeout: time.Duration(*httpClientTimeout) * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: *insecure},
			},
		},
	}
}

// Describe describes all the metrics ever exported by the cloudera exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.exporterScrapeError
	e.scrapeFailures.Describe(ch)

	e.clouderaUp.Describe(ch)

	ch <- e.license_expiration
	ch <- e.host_health_summary
	ch <- e.service_state
	ch <- e.service_health_summary
	ch <- e.flume_agent_scrape_error
	ch <- e.flume_agent_event_drain_success_count
	ch <- e.flume_agent_event_received_count
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {
	resp, err := e.client.Get(e.URI)
	if err != nil {
		e.clouderaUp.Set(0)
		ch <- prometheus.MustNewConstMetric(e.exporterScrapeError, prometheus.GaugeValue, 1)
		return fmt.Errorf("Error scraping cloudera: %v", err)
	}
	e.clouderaUp.Set(1)
	resp.Body.Close()

	// scrape license expiration
	ch <- prometheus.MustNewConstMetric(e.license_expiration, prometheus.GaugeValue, float64(globalLicenseExpiration))

	// scrape service states and service health summaries
	for c_services := 0; c_services < len(gjson.Get(globalServices, "items").Array()); c_services++ {
		var serviceState float64
		var serviceHealthSummary float64

		service := gjson.Get(globalServices, "items."+strconv.Itoa(c_services))

		if service.Get("serviceState").String() == "STARTED" {
			serviceState = 0
		} else {
			serviceState = 1
		}

		if service.Get("healthSummary").String() == "GOOD" {
			serviceHealthSummary = 0
		} else {
			serviceHealthSummary = 1
		}

		name := service.Get("name").String()
		displayName := service.Get("displayName").String()
		clusterName := service.Get("clusterRef.clusterName").String()

		ch <- prometheus.MustNewConstMetric(e.service_state, prometheus.GaugeValue, serviceState, name, displayName, clusterName)
		ch <- prometheus.MustNewConstMetric(e.service_health_summary, prometheus.GaugeValue, serviceHealthSummary, name, displayName, clusterName)
	}

	// scrape host health summaries
	for c_hosts := 0; c_hosts < len(globalHostsDetail); c_hosts++ {
		var hostHealthSummary float64

		host := gjson.Parse(globalHostsDetail[c_hosts])

		if host.Get("healthSummary").String() == "GOOD" {
			hostHealthSummary = 0
		} else {
			hostHealthSummary = 1
		}

		hostId := host.Get("hostId").String()
		hostname := host.Get("hostname").String()
		ipAddress := host.Get("ipAddress").String()

		ch <- prometheus.MustNewConstMetric(e.host_health_summary, prometheus.GaugeValue, hostHealthSummary, hostId, hostname, ipAddress)
	}

	// scrape flume agent metrics
	for _, f := range flumeMetrics {
		ch <- prometheus.MustNewConstMetric(e.flume_agent_scrape_error, prometheus.GaugeValue, f.errors, f.ip, f.port, f.name, f.displayname, f.cluster)
		for process, metrics := range f.json {
			if gjson.Parse(metrics.String()).Get("EventReceivedCount").Exists() {
				eventReceivedCount, _ := strconv.ParseFloat(gjson.Parse(metrics.String()).Get("EventReceivedCount").String(), 64)
				ch <- prometheus.MustNewConstMetric(e.flume_agent_event_received_count, prometheus.GaugeValue, eventReceivedCount, f.ip, f.port, f.name, f.displayname, f.cluster, process)
			}
			if gjson.Parse(metrics.String()).Get("EventDrainSuccessCount").Exists() {
				eventDrainSuccessCount, _ := strconv.ParseFloat(gjson.Parse(metrics.String()).Get("EventDrainSuccessCount").String(), 64)
				ch <- prometheus.MustNewConstMetric(e.flume_agent_event_drain_success_count, prometheus.GaugeValue, eventDrainSuccessCount, f.ip, f.port, f.name, f.displayname, f.cluster, process)
			}
		}
	}

	return nil
}

// Collect fetches the stats from configured cloudera location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock() // To protect metrics from concurrent collects.
	defer e.mutex.Unlock()
	if err := e.collect(ch); err != nil {
		log.Errorf("Error scraping cloudera: %s", err)
		e.scrapeFailures.Inc()
		e.scrapeFailures.Collect(ch)
	}
	e.clouderaUp.Collect(ch)
	return
}

// load content from URI (JSON)
func getContent(uri string) (body string, err error) {
	//fmt.Println("GETCONTENT: " + uri)
	scrapeError = 0

	httpClient := &http.Client{
		Timeout: time.Duration(*httpClientTimeout) * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: *insecure,
			},
		},
	}

	req, err := http.NewRequest(http.MethodGet, uri, nil)

	if err != nil {
		//scrapeError = 1
		log.Error(err)
		return "", err
	}

	req.SetBasicAuth(clouderauser, clouderapass)

	res, err := httpClient.Do(req)

	if err != nil {
		//scrapeError = 1
		//log.Error(err)
		//res.Body.Close()
		return "", err
	}

	if res.StatusCode < 200 || res.StatusCode >= 400 {
		log.Error(res.Status)
		//scrapeError = 1
		res.Body.Close()
		return "", errors.New("HTTP return code: " + strconv.Itoa(res.StatusCode))
	}

	content, err := ioutil.ReadAll(res.Body)

	if err != nil {
		//scrapeError = 1
		log.Error(err)
		res.Body.Close()
		return "", errors.New("Error while getting HTTP content / reading body.")
	}

	res.Body.Close()

	return string(content), err
}

// collect configuration from Cloudera API
func getConfigJson() {
	jobGetConfigJsonInProgress = true

	var localServicesDetail []string
	var localHostsDetail []string
	var localFlumeHosts []string
	var localFlumeRoleRefs []gjson.Result

	// get licence expiration
	bodyLicense, e := getContent(*clouderaApiUri + "/cm/license")
	if e != nil {
		//scrapeError = 1
		return
		//return nil, e
	}

	licenseExpirationTime, e := time.Parse("2006-01-02T03:04:05.000Z", gjson.Parse(bodyLicense).Get("expiration").String())
	if e != nil {
		log.Error(e)
	}

	localLicenseExpiration := licenseExpirationTime.Unix()

	// get hosts
	bodyHosts, e := getContent(*clouderaApiUri + "/hosts")
	if e != nil {
		//scrapeError = 1
		return
		//return nil, e
	}

	// get host details
	hostItems := gjson.Get(bodyHosts, "items")
	for c_hosts := 0; c_hosts < len(hostItems.Array()); c_hosts++ {
		bodyHealth, e := getContent(*clouderaApiUri + "/hosts/" + hostItems.Get(strconv.Itoa(c_hosts)+".hostId").String())
		if e != nil {
			//scrapeError = 1
			return
			//return nil, e
		}

		localHostsDetail = append(localHostsDetail, bodyHealth)
		health := gjson.Get(bodyHealth, "healthSummary")

		if health.String() == "GOOD" {
			localFlumeHosts = append(localFlumeHosts, hostItems.Get(strconv.Itoa(c_hosts)+".ipAddress").String())
			localFlumeRoleRefs = append(localFlumeRoleRefs, gjson.Get(bodyHealth, "roleRefs.#.serviceName"))
		}
	}

	// get clusters
	bodyClusters, e := getContent(*clouderaApiUri + "/clusters/")
	if e != nil {
		//scrapeError = 1
		log.Error(e)
		return
		//return nil, e
	}

	clustersItems := gjson.Get(bodyClusters, "items.#.displayName").Array()

	// get services in each cluster
	for c_clusters := 0; c_clusters < len(clustersItems); c_clusters++ {
		cluster := clustersItems[c_clusters].String()

		bodyServices, e := getContent(*clouderaApiUri + "/clusters/" + cluster + "/services/")
		if e != nil {
			//scrapeError = 1
			return
			//return nil, e
		}

		serviceItems := gjson.Get(bodyServices, "items")
		globalServices = bodyServices

		// get service details
		for c_services := 0; c_services < len(serviceItems.Array()); c_services++ {
			service := serviceItems.Get(strconv.Itoa(c_services))
			serviceType := service.Get("type").String()
			serviceName := service.Get("name").String()

			if serviceType == "FLUME" {
				flumeBody, e := getContent(*clouderaApiUri + "/clusters/" + cluster + "/services/" + serviceName + "/roleConfigGroups")
				if e != nil {
					//scrapeError = 1
					return
					//return nil, e
				}

				localServicesDetail = append(localServicesDetail, flumeBody)
			}
		}
	}

	globalLicenseExpiration = localLicenseExpiration
	globalHosts = bodyHosts
	flumeHosts = localFlumeHosts
	flumeRoleRefs = localFlumeRoleRefs
	globalServicesDetail = localServicesDetail
	globalHostsDetail = localHostsDetail

	jobGetConfigJsonInProgress = false
}

// collect Flume Agent Data
func getFlumeJson() {
	jobGetFlumeJsonInProgress = true

	flumeAgents := []FlumeAgent{}

	for c_services := 0; c_services < len(globalServices); c_services++ {
		service := gjson.Get(globalServices, "items").Get(strconv.Itoa(c_services))
		serviceType := service.Get("type").String()
		serviceName := service.Get("name").String()
		serviceDisplayName := service.Get("displayName").String()

		if serviceType == "FLUME" {
			for c_servicesdetail := 0; c_servicesdetail < len(globalServicesDetail); c_servicesdetail++ {
				servicedetail := gjson.Get(globalServicesDetail[c_servicesdetail], "items.#.serviceRef").Array()[0]
				servicedetailName := servicedetail.Get("serviceName").String()
				servicedetailCluster := servicedetail.Get("clusterName").String()

				if servicedetailName == serviceName {
					var port string

					flumeAgentPort := gjson.Get(globalServicesDetail[c_servicesdetail], "items.#.config.items.#[name==\"agent_http_port\"]#.value")

					if len(flumeAgentPort.Array()) == 1 {
						port = flumeAgentPort.Array()[0].Array()[0].String()
					} else {
						port = "41414"
					}

					for c_hosts := 0; c_hosts < len(flumeHosts); c_hosts++ {
						hosts := flumeRoleRefs[c_hosts].Array()

						for c_flumes := 0; c_flumes < len(hosts); c_flumes++ {
							if hosts[c_flumes].String() == serviceName {
								var flumeAgentScrapeError float64

								flumeAgentMetricsBody, e := getContent("http://" + flumeHosts[c_hosts] + ":" + port + "/metrics")
								if e != nil {
									//scrapeError = 1
									flumeAgentScrapeError = 1
									return
									//return nil, e
								} else {
									flumeAgentScrapeError = 0
								}

								flumeAgentJson := gjson.Parse(flumeAgentMetricsBody)
								flumeAgentMetrics := FlumeAgent{
									flumeHosts[c_hosts],
									port,
									servicedetailName,
									serviceDisplayName,
									servicedetailCluster,
									flumeAgentScrapeError,
									flumeAgentJson.Map(),
								}

								flumeAgents = append(flumeAgents, flumeAgentMetrics)
							}
						}
					}
				}
			}
		}
	}

	flumeMetrics = flumeAgents

	jobGetFlumeJsonInProgress = false
}

// Collect config from Cloudera API in the background.
// Only do it if it is not already running.
// Wait between every run like defined by clouderaApiScrapeInterval.
func jobGetConfigJson() {
	for {
		if jobGetConfigJsonInProgress == false {
			go func() {
				getConfigJson()
			}()
		}
		time.Sleep(time.Duration(*clouderaApiScrapeInterval) * time.Second)
	}
}

// Collect Flume Agent Data in the background.
// Only do it if it is not already running.
// Wait between every run like defined by clouderaFlumeScrapeInterval.
func jobGetFlumeJson() {
	for {
		if jobGetFlumeJsonInProgress == false {
			go func() {
				getFlumeJson()
			}()
		}
		time.Sleep(time.Duration(*clouderaFlumeScrapeInterval) * time.Second)
	}
}

func main() {
	flag.Parse()

	clouderauser = os.Getenv("CLOUDERAUSER")
	clouderapass = os.Getenv("CLOUDERAPASS")

	if clouderauser == "" || clouderapass == "" {
		log.Fatal("Please set Cloudera username and password!")
	}

	// do initial load of config from Cloudera API
	log.Infof("Loading config and metrics for the first time ...")
	log.Infof("  (This can take some time depending on the size of your cluster setup and your network speed.)")
	getConfigJson()
	getFlumeJson()
	log.Infof("... DONE")

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
	go func() {
		jobGetConfigJson()
	}()
	go func() {
		jobGetFlumeJson()
	}()

	log.Fatal(http.ListenAndServe(*listeningAddress, nil))
}
