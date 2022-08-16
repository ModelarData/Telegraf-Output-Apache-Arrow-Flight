# Apache Arrow Flight External Output Plugin

This plugin writes to [ModelarDB-RS](https://github.com/ModelarData/ModelarDB-RS) via the [Apache Arrow Flight protocol](https://arrow.apache.org/docs/format/Flight.html).

## Arrow Schema

The plugin currently supports inserting data points with the following schema:

```rust
 Field { name: "tid", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, 
 Field { name: "timestamp", data_type: Timestamp(Millisecond, None), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, 
 Field { name: "value", data_type: Float32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }
```

## Setting up and running the plugin

To build the binary and run the plugin:

1. Install the latest version of [Go](https://go.dev/doc/install).
2. Build the binary:
    * Windows: `go build -o binary/flight.exe cmd/main.go`
    * Linux/macOS: `go build -o binary/flight cmd/main.go`
3. Download the latest version of [telegraf](https://github.com/influxdata/telegraf/releases) for your platform. (To see which platform is needed, run the following command: `go env GOOS GOARCH`)
4. Extract the telegraf executable and configuration to the repository folder.
5. Configure the telegraf.conf file:
   * Search for `execd`, remove the comment in front of the tag `[[outputs.execd]]` and the option `command`.
     * Windows: Assign the following to `command`: `["./binary/flight.exe", "-config", "./plugins/output/flight/sample.conf"]` 
     * Linux/macOS: Assign the following to `command`: `["./binary/flight", "-config", "./plugins/output/flight/sample.conf"]` 
   * Configure any input plugin to consume metrics. (the metric must adhere to the schema presented in this README)
6. Run the plugin using telegraf: `telegraf --config telegraf.conf --input-filter mqtt_consumer --output-filter execd`

To run the tests: 
1. Install the latest version of [Go](https://go.dev/doc/install).
2. Run the command: `go test plugins/output/flight/flight_test.go plugins/output/flight/flight.go`


## Configuration

```toml @sample.conf
# Configuration for Arrow Flight to send metrics to.
[[outputs.flight]]
    ## Location to connect to.
    location = "0.0.0.0"

    ## Port to connect to.
    port = "9999"

    ## Table in which to insert time series data.
    table = "data"
```
## Known issues

* The descriptor is attached every time a payload is written to `DoPut()`.
* `GetSchema()` relies on a "hack" in ModelarDB-RS, because a bug is present in the Rust implementation of Apache Arrow Flight where the schema is not serialized properly.
* The schema is retrieved from the flight server, but there is currently nothing implemented to handle a schema other than the one described above, because a builder has to manually be initialized for the type of each field.
* Testing is still unstable.