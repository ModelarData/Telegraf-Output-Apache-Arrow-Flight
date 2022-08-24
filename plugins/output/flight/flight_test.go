package flight

import (
	_ "embed"
	"testing"

	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

const location = "0.0.0.0"
const servicePort = "9999"
const table = "data"

func TestConnectAndWrite(t *testing.T) {

	var err error

	if testing.Short() {
		t.Skip("Skipping integration testing in short mode")
	}

	f := &Flight{
		Location: location,
		Port:     servicePort,
	}

	err = f.Connect()
	require.NoError(t, err, "failed to connect to flight service")

	err = f.Write(testutil.MockMetrics())
	require.NoError(t, err, "failed to write to flight service")
}
