package flight

import (
	"context"
	_ "embed"
	"fmt"
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

func init() {
	outputs.Add("flight", func() telegraf.Output { return &Flight{} })
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

// Write should write immediately to the output, and not buffer writes
// (Telegraf manages the buffer for you). Returning an error will fail this
// batch of writes and the entire batch will be retried automatically.
func (f *Flight) Write(metrics []telegraf.Metric) error {

	// Create a new RecordBuilder using the schema.
	builder := array.NewRecordBuilder(memory.DefaultAllocator, &f.schema)
	defer builder.Release()

	schemaFields := f.schema.Fields()

	builder.Reserve(len(metrics))

	// Iterate through the metrics and add them to the RecordBuilder.
	for _, metric := range metrics {
		addMetricToRecordBuilder(schemaFields, builder, metric)
	}

	// Create a new Record from the RecordBuilder.
	rec := builder.NewRecord()
	defer rec.Release()

	return f.writer.Write(rec)
}

// Close the connection to the Apache Arrow Flight server.
func (f *Flight) Close() error {
	return f.writer.Close()
}

// Iterate through the fields in the schema,
// extract the tag or field values from the metric using getTag or getField,
// and add the corresponding value to the correct builder in RecordBuilder.
// If the value is not set in the metric, panic and print the error.
//
// The switches for the types inside each case are necessary because the type
// represented by the schema is not always the same as the type represented by the metric.
// For example, JSON does not define types and therefore parses all integers
// as floats. If the schema expects an integer in the field, Apache Arrow will panic.
func addMetricToRecordBuilder(schemaFields []arrow.Field, builder *array.RecordBuilder, metric telegraf.Metric) {

	for i, schemaField := range schemaFields {
		switch schemaField.Type.ID() {
		case arrow.TIMESTAMP:
			timeInt := metric.Time().UnixMilli()
			builder.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(timeInt))
		case arrow.STRING:
			metricTag := getTag(metric, schemaField, i)
			builder.Field(i).(*array.StringBuilder).Append(metricTag)
		case arrow.INT32:
			metricField := getField(metric, schemaField, i)
			switch value := metricField.(type) {
			case int32:
				builder.Field(i).(*array.Int32Builder).Append(value)
			case int64:
				builder.Field(i).(*array.Int32Builder).Append(int32(value))
			case float32:
				builder.Field(i).(*array.Int32Builder).Append(int32(value))
			case float64:
				builder.Field(i).(*array.Int32Builder).Append(int32(value))
			}
		case arrow.INT64:
			metricField := getField(metric, schemaField, i)
			switch value := metricField.(type) {
			case int32:
				builder.Field(i).(*array.Int64Builder).Append(int64(value))
			case int64:
				builder.Field(i).(*array.Int64Builder).Append(value)
			case float32:
				builder.Field(i).(*array.Int64Builder).Append(int64(value))
			case float64:
				builder.Field(i).(*array.Int64Builder).Append(int64(value))
			}
		case arrow.FLOAT32:
			metricField := getField(metric, schemaField, i)
			switch value := metricField.(type) {
			case int32:
				builder.Field(i).(*array.Float32Builder).Append(float32(value))
			case int64:
				builder.Field(i).(*array.Float32Builder).Append(float32(value))
			case float32:
				builder.Field(i).(*array.Float32Builder).Append(value)
			case float64:
				builder.Field(i).(*array.Float32Builder).Append(float32(value))
			}
		case arrow.FLOAT64:
			metricField := getField(metric, schemaField, i)
			switch value := metricField.(type) {
			case int32:
				builder.Field(i).(*array.Float64Builder).Append(float64(value))
			case int64:
				builder.Field(i).(*array.Float64Builder).Append(float64(value))
			case float32:
				builder.Field(i).(*array.Float64Builder).Append(float64(value))
			case float64:
				builder.Field(i).(*array.Float64Builder).Append(value)
			}
		}
	}
}

// Return the value of the metric tag with the name equal to the name of the schema field.
func getTag(metric telegraf.Metric, schemaField arrow.Field, i int) string {
	metricTag, wasSet := metric.GetTag(schemaField.Name)
	// If the tag is not set, the program will panic.
	// This is to prevent the plugin from attempting a retransmit.
	// a retransmit if the recuired value is missing.
	if !wasSet {
		panic(fmt.Sprintf("tag %d : %s not set", i, schemaField.Name))
	}
	return metricTag
}

// Return the value of the metric field with the name equal to the name of the schema field.
func getField(metric telegraf.Metric, schemaField arrow.Field, i int) interface{} {
	metricField, wasSet := metric.GetField(schemaField.Name)
	// If the field is not set, the program will panic.
	// This is to prevent the plugin from attempting
	// a retransmit if the recuired value is missing.
	if !wasSet {
		panic(fmt.Sprintf("field %d : %s not set", i, schemaField.Name))
	}
	return metricField
}
