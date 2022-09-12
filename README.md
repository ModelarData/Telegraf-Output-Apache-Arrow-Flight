# Apache Arrow Flight Telegraf Output Plugin

This Telegraf output plugin is a general purpose output plugin for the [Apache Arrow Flight protocol](https://arrow.apache.org/docs/format/Flight.html).

## Overview

The plugin currently supports outputting Telegraf metrics to the Apache Arrow Flight server and table specified in the [sample configuration](/plugins/output/flight/sample.conf).
It uses the type of each column in the table to determine which part of the metrics should be assigned to that column.
If the column has the type `TIMESTAMP` `metric.Time().UnixMilli()` is assigned to it, if the column has the type `STRING` the tag with the same name is assigned to it, for columns of all other types the field with the same name is assigned to it.

## Installation

To build the binary and run the plugin:

1. Install the latest version of [Go](https://go.dev/doc/install).
2. Build the binary:
    * Windows: `go build -o binary/flight.exe cmd/main.go`
    * Linux/macOS: `go build -o binary/flight cmd/main.go`
3. Download the latest version of [Telegraf](https://portal.influxdata.com/downloads/) for your platform. (To see which platform is needed, run the following command: `go env GOOS GOARCH`)
4. Extract the Telegraf binaries and configuration to the repository folder.
5. Configure the `telegraf.conf` file:
   * In `telegraf.conf`, remove the comment in front of the tag `[[outputs.execd]]` and the option `command`.
     * Windows: Assign the following to `command` so the resulting line becomes:
      `command = ["/path/to/flight.exe", "-config", "/path/to/sample.conf"]`
     * Linux/macOS: Assign the following to `command` so the resulting line becomes:
      `command = ["/path/to/flight", "-config", "/path/to/sample.conf"]`
   * Configure any input plugin to consume metrics. (The metric must adhere to the schema presented in this README)
6. Update the [sample configuration](/plugins/output/flight/sample.conf) to specify the Apache Arrow Flight server to connect to and the table to insert metrics into.
7. Run the plugin using Telegraf: `telegraf --config telegraf.conf --input-filter chosen_input_plugin --output-filter execd`

To run the tests:
1. Install the latest version of [Go](https://go.dev/doc/install).
2. Start an Apache Arrow Flight Server on port 9999.
3. Run the command: `go test plugins/output/flight/flight_test.go plugins/output/flight/flight.go`


## Configuration

The following configuration is a [sample configuration](/plugins/output/flight/sample.conf) used to specify the Arrow Flight server to connect to and the table to insert metrics into.

```toml @sample.conf
## Configuration for where the Arrow Flight Client will send metrics to.
[[outputs.flight]]
    ## URL to connect to.
    location = "0.0.0.0"

    ## Port to connect to.
    port = "9999"

    ## Name of the table to store the metrics in.
    table = "data"
```

## License

The Apache Arrow Flight Telegraf Output Plugin is licensed under version 2.0 of the Apache License and a copy of the license is bundled with the program.
