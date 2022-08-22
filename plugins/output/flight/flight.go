package flight

import (
	"context"
	_ "embed"
	"net"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/flight"
	"github.com/apache/arrow/go/v9/arrow/ipc"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

// SampleConfig returns the configuration of the Apache Arrow Flight plugin.
func (*Flight) SampleConfig() string {
	return sampleConfig
}

// Flight contains the configuration, data structures, and interfaces for the Apache Arrow Flight plugin.
// It is the primary data structure for the plugin.
type Flight struct {
	Location string `toml:"location"`
	Port     string `toml:"port"`
	Table    string `toml:"table"`

	client flight.FlightServiceClient
	writer flight.Writer
	schema arrow.Schema
	ctx    context.Context
	desc   *flight.FlightDescriptor
}

// Write should write immediately to the output, and not buffer writes
// (Telegraf manages the buffer for you). Returning an error will fail this
// batch of writes and the entire batch will be retried automatically.
func (f *Flight) Write(metrics []telegraf.Metric) error {

	builder := array.NewRecordBuilder(memory.DefaultAllocator, &f.schema)
	defer builder.Release()

	// Currently, the plugin only implements support for the simplest schema
	// supported by legacy JVM and current Rust versions of [ModelarDB](https://github.com/ModelarData/ModelarDB-RS),
	// as listed below. Support for an arbitrary schema is planned.

	tidBuilder := builder.Field(0).(*array.Int32Builder) // Unused and deprecated time series identifier.
	timeBuilder := builder.Field(1).(*array.TimestampBuilder)
	valueBuilder := builder.Field(2).(*array.Float32Builder)

	builder.Reserve(len(metrics))

	for _, m := range metrics {

		tidBuilder.AppendValues([]int32{1}, nil)

		timeInt := m.Time().UnixMilli()

		timeBuilder.AppendValues([]arrow.Timestamp{arrow.Timestamp(timeInt)}, nil)

		for _, field := range m.FieldList() {
			valueBuilder.AppendValues([]float32{float32(field.Value.(float64))}, nil)
		}
	}

	rec := builder.NewRecord()
	defer rec.Release()

	return f.writer.Write(rec)
}

// Connect to the Apache Arrow Flight server. If an error is at any point returned
// it will be written to the output and the plugin will attempt to restart.
func (f *Flight) Connect() error {

	// Create a new connection to the gRPC server using the given target.
	conn, err := grpc.Dial(net.JoinHostPort(f.Location, f.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return err
	}

	// Create an empty context and add it to the Flight struct.
	f.ctx = context.Background()

	// Initialize a new Flight Service Client using the client API and add it to the Flight struct.
	f.client = flight.NewFlightServiceClient(conn)

	// Read the table value from the configuration file and add it to the Flight Descriptor in the Flight struct.
	f.desc = &flight.FlightDescriptor{
		Type: 1,
		Path: []string{f.Table},
	}

	// Retrieve the schema from the server, deserialize it and add it to the Flight struct.
	getSchema, err := f.client.GetSchema(f.ctx, f.desc)

	if err != nil {
		return err
	}

	retrievedSchema := getSchema.GetSchema()

	deserializedSchema, err := flight.DeserializeSchema(retrievedSchema, memory.DefaultAllocator)

	if err != nil {
		return err
	}

	f.schema = *deserializedSchema

	// Push a new DoPut stream to the server using the Flight Service Client.
	stream, err := f.client.DoPut(f.ctx)

	if err != nil {
		return err
	}

	f.writer = *flight.NewRecordWriter(stream, ipc.WithSchema(&f.schema), ipc.WithAllocator(memory.DefaultAllocator))

	f.writer.SetFlightDescriptor(f.desc)

	return nil
}

// Close the connection to the Apache Arrow Flight server.
func (f *Flight) Close() error {
	return f.writer.Close()
}

func init() {
	outputs.Add("flight", func() telegraf.Output { return &Flight{} })
}
