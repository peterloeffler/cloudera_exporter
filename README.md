# cloudera_exporter

This exporter collects Cloudera metrics.  
Because there can be very much to fetch via HTTP depending on your Cloudera setup this exporter is able to call the Cloudera API as well as the Flume Agents in the background.  
To enable this feature use the parameters --cloudera.api.scrape.interval AND --cloudera.flume.scrape.interval.  
That means that the metric values are not collected when the agent is scraped.  

## Building and running

    go get github.com/peterloeffler/cloudera_exporter

To authenticate to the Cloudera API you have to set username and password in your environment:

    export CLOUDERAUSER=xxxxxx
    export CLOUDERAPASS=xxxxxx

    ${GOPATH-$HOME/go}/bin/cloudera_exporter

## Optional parameters

### --cloudera.api.scrape.interval (int)
Interval to scrape the Cloudera API in seconds for scraping in the background (don't forget to also set -cloudera.flume.scrape.interval).
### --cloudera.api.uri (string)
Cloudera API URI (default "http://localhost:7180/api/v6")
### --cloudera.flume.scrape.interval (int)
Interval to scrape the Cloudera Flume Agents in seconds for scraping in the background (don't forget to also set -clouderaApiScrapeInterval).
### --http.client.timeout (int)
Timeout for the go http client in seconds for the request to the Cloudera API and the Flume Agents. (default 10)
### --http.client.insecure
Ignore server certificate if using https (default true)
### --telemetry.address (string)
Address on which to expose metrics. (default ":9512")
### --telemetry.endpoint (string)
Path under which to expose metrics. (default "/metrics")
