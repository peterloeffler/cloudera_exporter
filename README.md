# cloudera_exporter

This exporter collects Cloudera metrics.  
Because there can be very much to fetch via HTTP depending on your Cloudera setup this exporter calls the Cloudera API as well as the Flume Agents in the background.  
That means that the metric values are not collected when the agent is scraped.  
You can configure the intervals for API and Flume Agent calls by setting the parameters -cloudera.api.scrape.interval and -cloudera.flume.scrape.interval.

## Building and running

    go get github.com/peterloeffler/cloudera_exporter

To authenticate to the Cloudera API you have to set username and password in your environment:

    export CLOUDERAUSER=xxxxxx
    export CLOUDERAPASS=xxxxxx

    ${GOPATH-$HOME/go}/bin/cloudera_exporter
