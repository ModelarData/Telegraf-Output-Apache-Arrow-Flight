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

// DO NOT REMOVE THE NEXT TWO LINES! This is required to embed the sampleConfig data.
//
//go:embed sample.conf
var sampleConfig string

func (*Flight) SampleConfig() string {
	return sampleConfig
}

type Flight struct {
	Location string `toml:"location"`
	Port     string `toml:"port"`

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
func (s *Flight) Write(metrics []telegraf.Metric) error {

	for _, m := range metrics {

		timeInt := m.Time().UnixMilli()

		s.timeStamps = append(s.timeStamps, arrow.Timestamp(timeInt))

		for _, field := range m.FieldList() {
			s.values = append(s.values, float32(field.Value.(float64)))
		}

	}

	b := array.NewRecordBuilder(&s.pool, &s.schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	b.Field(1).(*array.TimestampBuilder).AppendValues(s.timeStamps, nil)
	b.Field(2).(*array.Float32Builder).AppendValues(s.values, nil)

	rec := b.NewRecord()
	defer rec.Release()

	err := s.writer.Write(rec)

	if err != nil {
		log.Fatal(err)
	}

	s.timeStamps = nil

	s.values = nil

	return nil
}

// Init is for setup, and validating config.
func (s *Flight) Init() error {

	return nil
}

// Make any connection required here
func (s *Flight) Connect() error {

	conn, err := grpc.Dial(net.JoinHostPort(s.Location, s.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatal(err)
	}

	s.ctx = context.Background()

	s.client = flight.NewFlightServiceClient(conn)

	if err != nil {
		log.Fatal(err)
	}

	flights, err := s.client.ListFlights(s.ctx, &flight.Criteria{})

	if err != nil {
		log.Fatal(err)
	}

	flightsInfo, err := flights.Recv()

	if err != nil {
		log.Fatal(err)
	}

	flightDescriptor := flightsInfo.GetFlightDescriptor()

	s.desc = flightDescriptor

	getSchema, err := s.client.GetSchema(s.ctx, s.desc)

	if err != nil {
		log.Fatal(err)
	}

	retrievedSchema := getSchema.GetSchema()

	deserializedSchema, err := flight.DeserializeSchema(retrievedSchema, memory.DefaultAllocator)

	if err != nil {
		log.Fatal(err)
	}

	s.schema = *deserializedSchema

	if err != nil {
		log.Fatal(err)
	}

	stream, err := s.client.DoPut(s.ctx)

	if err != nil {
		log.Fatal(err)
	}

	s.pool = *memory.NewGoAllocator()

	s.writer = *flight.NewRecordWriter(stream, ipc.WithSchema(&s.schema), ipc.WithAllocator(&s.pool))

	s.writer.SetFlightDescriptor(s.desc)

	return nil
}

func (s *Flight) Close() error {
	// Close any connections here.
	// Write will not be called once Close is called, so there is no need to synchronize.
	s.writer.Close()
	return nil
}

func init() {
	outputs.Add("flight", func() telegraf.Output { return &Flight{} })
}
