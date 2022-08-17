package flight

import (
	"context"
	_ "embed"
	"log"
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

	timeStamps []arrow.Timestamp
	values     []float32

	client flight.FlightServiceClient
	writer flight.Writer
	schema arrow.Schema
	ctx    context.Context
	pool   memory.GoAllocator
	desc   *flight.FlightDescriptor
}

// Write should write immediately to the output, and not buffer writes
// (Telegraf manages the buffer for you). Returning an error will fail this
// batch of writes and the entire batch will be retried automatically.
func (f *Flight) Write(metrics []telegraf.Metric) error {

	for _, m := range metrics {

		timeInt := m.Time().UnixMilli()

		f.timeStamps = append(f.timeStamps, arrow.Timestamp(timeInt))

		for _, field := range m.FieldList() {
			f.values = append(f.values, float32(field.Value.(float64)))
		}

	}

	builder := array.NewRecordBuilder(&f.pool, &f.schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	builder.Field(1).(*array.TimestampBuilder).AppendValues(f.timeStamps, nil)
	builder.Field(2).(*array.Float32Builder).AppendValues(f.values, nil)

	rec := builder.NewRecord()
	defer rec.Release()

	err := f.writer.Write(rec)

	if err != nil {
		log.Fatal(err)
	}

	f.timeStamps = nil

	f.values = nil

	return nil
}

// Make any connection required here
func (f *Flight) Connect() error {

	conn, err := grpc.Dial(net.JoinHostPort(f.Location, f.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatal(err)
	}

	f.ctx = context.Background()

	f.client = flight.NewFlightServiceClient(conn)

	if err != nil {
		log.Fatal(err)
	}

	f.desc = &flight.FlightDescriptor{
		Type: 1,
		Path: []string{f.Table},
	}

	getSchema, err := f.client.GetSchema(f.ctx, f.desc)

	if err != nil {
		log.Fatal(err)
	}

	retrievedSchema := getSchema.GetSchema()

	deserializedSchema, err := flight.DeserializeSchema(retrievedSchema, memory.DefaultAllocator)

	if err != nil {
		log.Fatal(err)
	}

	f.schema = *deserializedSchema

	if err != nil {
		log.Fatal(err)
	}

	stream, err := f.client.DoPut(f.ctx)

	if err != nil {
		log.Fatal(err)
	}

	f.pool = *memory.NewGoAllocator()

	f.writer = *flight.NewRecordWriter(stream, ipc.WithSchema(&f.schema), ipc.WithAllocator(&f.pool))

	f.writer.SetFlightDescriptor(f.desc)

	return nil
}

// Close any connections here.
func (f *Flight) Close() error {
	f.writer.Close()
	return nil
}

func init() {
	outputs.Add("flight", func() telegraf.Output { return &Flight{} })
}
