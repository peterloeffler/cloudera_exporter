package main

import (
	"crypto/tls"
	"errors"
	"github.com/prometheus/common/log"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

// load content from URI (JSON)
func getContent(uri string) (body string, err error) {
	log.Debug("get content from " + uri)
	scrapeError = 0

	httpClient := &http.Client{
		Timeout: time.Duration(*httpClientTimeout) * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: *httpClientInsecure,
			},
		},
	}

	req, err := http.NewRequest(http.MethodGet, uri, nil)

	if err != nil {
		log.Error(err)
		return "", err
	}

	req.SetBasicAuth(clouderauser, clouderapass)

	res, err := httpClient.Do(req)

	if err != nil {
		return "", err
	}

	if res.StatusCode < 200 || res.StatusCode >= 400 {
		log.Error(res.Status)
		res.Body.Close()
		return "", errors.New("HTTP return code: " + strconv.Itoa(res.StatusCode))
	}

	content, err := ioutil.ReadAll(res.Body)

	if err != nil {
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

	var localLicenseExpirationLastScrape float64
	var localHostsDetail []string

	clusterMembers := []ClusterMember{}
	services := []Service{}

	// get licence expiration
	bodyLicense, e := getContent(*clouderaApiUri + "/cm/license")
	if e != nil {
		return
	} else {
		localLicenseExpirationLastScrape = float64(time.Now().Unix())
	}

	licenseExpirationTime, e := time.Parse("2006-01-02T03:04:05.000Z", gjson.Parse(bodyLicense).Get("expiration").String())
	if e != nil {
		log.Error(e)
	}

	localLicenseExpiration := licenseExpirationTime.Unix()

	// get hosts
	bodyHosts, e := getContent(*clouderaApiUri + "/hosts")
	if e != nil {
		return
	}

	// get host details
	var hostHealthSummary float64
	hostItems := gjson.Get(bodyHosts, "items")
	for c_hosts := 0; c_hosts < len(hostItems.Array()); c_hosts++ {
		bodyHostsDetail, e := getContent(*clouderaApiUri + "/hosts/" + hostItems.Get(strconv.Itoa(c_hosts)+".hostId").String())
		if e != nil {
			return
		}

		localHostsDetail = append(localHostsDetail, bodyHostsDetail)

		switch gjson.Get(bodyHostsDetail, "healthSummary").String() {
		case "GOOD":
			hostHealthSummary = 0
		case "BAD":
			hostHealthSummary = 1
		case "DISABLED":
			hostHealthSummary = 2
		case "HISTORY_NOT_AVAILABLE":
			hostHealthSummary = 3
		case "NOT_AVAILABLE":
			hostHealthSummary = 4
		case "CONCERNING":
			hostHealthSummary = 5
		default:
			hostHealthSummary = -1
		}

		hostId := gjson.Get(bodyHostsDetail, "hostId").String() // can be done different (comes from above)
		hostname := gjson.Get(bodyHostsDetail, "hostname").String()
		ipAddress := gjson.Get(bodyHostsDetail, "ipAddress").String()
		services := gjson.Get(bodyHostsDetail, "roleRefs.#.serviceName").Array()

		clusterMemberMetrics := ClusterMember{
			hostId,
			hostname,
			ipAddress,
			services,
			hostHealthSummary,
			float64(time.Now().Unix()),
		}

		clusterMembers = append(clusterMembers, clusterMemberMetrics)
	}

	// get clusters
	bodyClusters, e := getContent(*clouderaApiUri + "/clusters/")
	if e != nil {
		log.Error(e)
		return
	}

	clustersItems := gjson.Get(bodyClusters, "items.#.displayName").Array()

	// get services in each cluster
	for c_clusters := 0; c_clusters < len(clustersItems); c_clusters++ {
		cluster := clustersItems[c_clusters].String()

		bodyServices, e := getContent(*clouderaApiUri + "/clusters/" + cluster + "/services/")
		if e != nil {
			return
		}

		serviceItems := gjson.Get(bodyServices, "items")

		// get service details
		for c_services := 0; c_services < len(serviceItems.Array()); c_services++ {
			var serviceState float64
			var serviceHealth float64
			var servicePort string

			service := serviceItems.Get(strconv.Itoa(c_services))
			serviceName := service.Get("name").String()
			serviceDisplayName := service.Get("displayName").String()
			serviceType := service.Get("type").String()
			serviceStateString := service.Get("serviceState").String()
			serviceHealthString := service.Get("healthSummary").String()

			if serviceType == "FLUME" {
				serviceDetailBody, e := getContent(*clouderaApiUri + "/clusters/" + cluster + "/services/" + serviceName + "/roleConfigGroups")
				if e != nil {
					return
				}

				if len(gjson.Get(serviceDetailBody, "items.#.config.items.#[name==\"agent_http_port\"]#.value").Array()) > 0 {
					if len(gjson.Get(serviceDetailBody, "items.#.config.items.#[name==\"agent_http_port\"]#.value").Array()[0].Array()) > 0 {
						servicePort = gjson.Get(serviceDetailBody, "items.#.config.items.#[name==\"agent_http_port\"]#.value").Array()[0].Array()[0].String()
					}
				}
			}

			switch serviceStateString {
			case "STARTED":
				serviceState = 0
			case "STOPPED":
				serviceState = 1
			case "HISTORY_NOT_AVAILABLE":
				serviceState = 2
			case "UNKNOWN":
				serviceState = 3
			case "STARTING":
				serviceState = 4
			case "STOPPING":
				serviceState = 5
			case "NA":
				serviceState = 6
			default:
				serviceState = -1
			}

			switch serviceHealthString {
			case "GOOD":
				serviceHealth = 0
			case "BAD":
				serviceHealth = 1
			case "DISABLED":
				serviceHealth = 2
			case "HISTORY_NOT_AVAILABLE":
				serviceHealth = 3
			case "NOT_AVAILABLE":
				serviceHealth = 4
			case "CONCERNING":
				serviceHealth = 5
			default:
				serviceHealth = -1
			}

			serviceMetrics := Service{
				serviceName,
				serviceDisplayName,
				serviceType,
				servicePort,
				cluster,
				serviceState,
				serviceHealth,
				float64(time.Now().Unix()),
			}

			services = append(services, serviceMetrics)
		}
	}

	globalLicenseExpiration = localLicenseExpiration
	globalLicenseExpirationLastScrape = localLicenseExpirationLastScrape
	globalClusterMemberMetrics = clusterMembers
	globalServiceMetrics = services

	jobGetConfigJsonInProgress = false
}

// collect Flume Agent Data
func getFlumeJson() {
	jobGetFlumeJsonInProgress = true

	flumeAgents := []FlumeAgent{}

	for _, s := range globalServiceMetrics {
		if s.servicetype == "FLUME" {
			for _, cm := range globalClusterMemberMetrics {
				if cm.health == 0 {
					for c_flumes := 0; c_flumes < len(cm.services); c_flumes++ {
						if cm.services[c_flumes].String() == s.name {
							var flumeAgentScrapeError float64
							var port string

							portInt, _ := strconv.Atoi(s.port)
							if portInt <= 0 {
								port = "41414"
							} else {
								port = s.port
							}

							flumeAgentMetricsBody, e := getContent("http://" + cm.ip + ":" + port + "/metrics")
							if e != nil {
								flumeAgentScrapeError = 1
								return
							} else {
								flumeAgentScrapeError = 0
							}

							flumeAgentMetrics := FlumeAgent{
								cm.ip,
								port,
								s.name,
								s.displayname,
								s.cluster,
								flumeAgentScrapeError,
								float64(time.Now().Unix()),
								gjson.Parse(flumeAgentMetricsBody).Map(),
							}

							flumeAgents = append(flumeAgents, flumeAgentMetrics)
						}
					}
				}
			}
		}
	}

	globalFlumeMetrics = flumeAgents

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
