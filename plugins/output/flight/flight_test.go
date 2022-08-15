package flight

import (
	"testing"

	"github.com/influxdata/telegraf/testutil"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stretchr/testify/require"
)

const servicePort = "9999"

func TestConnectAndWriteIntegration(t *testing.T) {

	var err error

	if testing.Short() {
		t.Skip("Skipping integration testing in short mode")
	}

	container := testutil.Container{
		Image:        "apache/arrow-dev:amd64-debian-11-go-1.18-cgo",
		ExposedPorts: []string{servicePort},
		WaitingFor:   wait.ForListeningPort(servicePort),
		Address:      "0.0.0.0",
	}

	err = container.Start()

	require.NoError(t, err, "failed to start container")

	defer func() {
		require.NoError(t, container.Terminate(), "terminating the container has failed")
	}()

	f := &Flight{
		Location: container.Address,
		Port:     servicePort,
	}

	err = f.Connect()
	require.NoError(t, err, "failed to connect to flight service")
}
