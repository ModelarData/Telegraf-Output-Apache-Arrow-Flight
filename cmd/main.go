package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/agneborn98/Arrow-Flight-ModelarDB-Output-Plugin/plugins/output/flight"

	"github.com/influxdata/telegraf/plugins/common/shim"
)

func main() {

	var pollInterval = flag.Duration("poll_interval", 1*time.Second, "how often to send metrics(in seconds)")
	var pollIntervalDisabled = flag.Bool("poll_interval_disabled", false, "set to true to disable polling. You want to use this when you are sending metrics on your own schedule")
	var configFile = flag.String("config", "", "path to the config file for this plugin")
	var err error

	// Parse command line options.
	flag.Parse()
	if *pollIntervalDisabled {
		*pollInterval = shim.PollIntervalDisabled
	}

	shimLayer := shim.New()

	err = shimLayer.LoadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Err loading input: %s\n", err)
		os.Exit(1)
	}

	if err := shimLayer.Run(*pollInterval); err != nil {
		fmt.Fprintf(os.Stderr, "Err: %s\n", err)
		os.Exit(1)
	}
}
