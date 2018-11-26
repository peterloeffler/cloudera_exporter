package main

import (
        "crypto/tls"
        "fmt"
        "github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/common/log"
        "github.com/tidwall/gjson"
        "net/http"
        "strconv"
        "time"
)

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
		license_expiration_last_scrape: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "license_expiration_last_scrape"),
			"When Cloudera License Expiration was last scraped. 0 means last scrape was not successful.",
			nil, nil,
		),
		host_health_summary: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "host_health_summary"),
			"Cloudera Host Health Summary. 0 is GOOD, 1 is something else.",
			hostLabelNames, nil,
		),
		host_health_summary_last_scrape: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "host_health_summary_last_scrape"),
			"When Cloudera Host Health Summary was last scraped. 0 means last scrape was not successful.",
			hostLabelNames, nil,
		),
		service_state: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "service_state"),
			"Cloudera Service State. 0 is STARTED, 1 is something else.",
			serviceLabelNames, nil,
		),
		service_state_last_scrape: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "service_state_last_scrape"),
			"When Cloudera Service State was last scraped. 0 means last scrape was not successful.",
			serviceLabelNames, nil,
		),
		service_health_summary: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "service_health_summary"),
			"Cloudera Service Health Summary. 0 is GOOD, 1 is something else.",
			serviceLabelNames, nil,
		),
		service_health_summary_last_scrape: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "service_health_summary_last_scrape"),
			"When Cloudera Service Health Summary was last scraped. 0 means last scrape was not successful.",
			serviceLabelNames, nil,
		),
		flume_agent_scrape_error: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "flume_agent_scrape_error"),
			"Cloudera Flume Agent Scrape Error. 0 is OK.",
			flumeErrorLabelNames, nil,
		),
		flume_agent_last_scrape: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "flume_agent_last_scrape"),
			"When Cloudera Flume Agent was last scraped. 0 means last scrape was not successful.",
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
				TLSClientConfig: &tls.Config{InsecureSkipVerify: *httpClientInsecure},
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
	ch <- e.license_expiration_last_scrape
	ch <- e.host_health_summary
	ch <- e.host_health_summary_last_scrape
	ch <- e.service_state
	ch <- e.service_state_last_scrape
	ch <- e.service_health_summary
	ch <- e.service_health_summary_last_scrape
	ch <- e.flume_agent_scrape_error
	ch <- e.flume_agent_last_scrape
	ch <- e.flume_agent_event_drain_success_count
	ch <- e.flume_agent_event_received_count
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {
	if *clouderaApiScrapeInterval == 0 {
		getConfigJson()
	}
	if *clouderaFlumeScrapeInterval == 0 {
		getFlumeJson()
	}

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
	ch <- prometheus.MustNewConstMetric(e.license_expiration_last_scrape, prometheus.GaugeValue, globalLicenseExpirationLastScrape)

	// scrape service states and service health summaries
	for _, s := range globalServiceMetrics {
		ch <- prometheus.MustNewConstMetric(e.service_state, prometheus.GaugeValue, s.state, s.name, s.displayname, s.cluster)
		ch <- prometheus.MustNewConstMetric(e.service_state_last_scrape, prometheus.GaugeValue, s.lastscrape, s.name, s.displayname, s.cluster)
		ch <- prometheus.MustNewConstMetric(e.service_health_summary, prometheus.GaugeValue, s.health, s.name, s.displayname, s.cluster)
		ch <- prometheus.MustNewConstMetric(e.service_health_summary_last_scrape, prometheus.GaugeValue, s.lastscrape, s.name, s.displayname, s.cluster)
	}

	// scrape host health summaries
	for _, cm := range globalClusterMemberMetrics {
		ch <- prometheus.MustNewConstMetric(e.host_health_summary, prometheus.GaugeValue, cm.health, cm.id, cm.name, cm.ip)
		ch <- prometheus.MustNewConstMetric(e.host_health_summary_last_scrape, prometheus.GaugeValue, cm.lastscrape, cm.id, cm.name, cm.ip)
	}

	// scrape flume agent metrics
	for _, f := range globalFlumeMetrics {
		ch <- prometheus.MustNewConstMetric(e.flume_agent_scrape_error, prometheus.GaugeValue, f.errors, f.ip, f.port, f.name, f.displayname, f.cluster)
		ch <- prometheus.MustNewConstMetric(e.flume_agent_last_scrape, prometheus.GaugeValue, f.lastscrape, f.ip, f.port, f.name, f.displayname, f.cluster)
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
