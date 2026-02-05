// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package listener

import (
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Helper functions

func newTestTCPListener() (*StatsDTCPListener, chan string, prometheus.Counter) {
	packetBuffer := make(chan string, 10)
	truncatedPackets := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_truncated_packets_total",
	})

	return &StatsDTCPListener{
		Logger:           log.NewNopLogger(),
		PacketBuffer:     packetBuffer,
		TCPConnections:   prometheus.NewCounter(prometheus.CounterOpts{}),
		TCPErrors:        prometheus.NewCounter(prometheus.CounterOpts{}),
		TCPLineTooLong:   prometheus.NewCounter(prometheus.CounterOpts{}),
		TruncatedPackets: truncatedPackets,
	}, packetBuffer, truncatedPackets
}

func sendTCPData(t *testing.T, addr net.Addr, data []byte) {
	conn, err := net.DialTCP("tcp", nil, addr.(*net.TCPAddr))
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write(data)
	require.NoError(t, err)
}

func getCounterValue(counter prometheus.Counter) float64 {
	var m dto.Metric
	if err := counter.Write(&m); err != nil {
		return 0.0
	}
	return m.Counter.GetValue()
}

// Tests

func TestTCPLineWithoutNewline(t *testing.T) {
	lc, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer lc.Close()

	listener, packets, truncated := newTestTCPListener()
	listener.Conn = lc

	go sendTCPData(t, lc.Addr(), []byte("foo:1|c"))

	conn, err := lc.AcceptTCP()
	require.NoError(t, err)
	listener.HandleConn(conn)

	require.Equal(t, 0, len(packets))
	require.Equal(t, float64(1), getCounterValue(truncated))
}

func TestTCPMultipleLinesInOnePacket(t *testing.T) {
	lc, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer lc.Close()

	listener, packets, _ := newTestTCPListener()
	listener.Conn = lc

	go sendTCPData(t, lc.Addr(), []byte("foo:1|c\nbar:2|g\nbaz:3|ms\n"))

	conn, err := lc.AcceptTCP()
	require.NoError(t, err)

	done := make(chan bool)
	go func() {
		listener.HandleConn(conn)
		done <- true
	}()

	time.Sleep(50 * time.Millisecond)

	require.Equal(t, 3, len(packets))
	require.Equal(t, "foo:1|c\n", <-packets)
	require.Equal(t, "bar:2|g\n", <-packets)
	require.Equal(t, "baz:3|ms\n", <-packets)
}

func TestTCPMultipleLinesWithTrailingFragment(t *testing.T) {
	lc, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer lc.Close()

	listener, packets, truncated := newTestTCPListener()
	listener.Conn = lc

	go sendTCPData(t, lc.Addr(), []byte("foo:1|c\nbar:2|g\nincomplete"))

	conn, err := lc.AcceptTCP()
	require.NoError(t, err)
	listener.HandleConn(conn)

	require.Equal(t, 2, len(packets))
	require.Equal(t, "foo:1|c\n", <-packets)
	require.Equal(t, "bar:2|g\n", <-packets)
	require.Equal(t, float64(1), getCounterValue(truncated))
}

func TestTCPStreamingData(t *testing.T) {
	lc, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer lc.Close()

	listener, packets, _ := newTestTCPListener()
	listener.Conn = lc

	// Send data in chunks that don't align with line boundaries
	go func() {
		conn, err := net.DialTCP("tcp", nil, lc.Addr().(*net.TCPAddr))
		require.NoError(t, err)
		defer conn.Close()

		writes := [][]byte{
			[]byte("foo:1"),
			[]byte("|c\nbar:2|"),
			[]byte("g\nbaz:3|ms\n"),
		}
		for _, data := range writes {
			_, err = conn.Write(data)
			require.NoError(t, err)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	conn, err := lc.AcceptTCP()
	require.NoError(t, err)

	done := make(chan bool)
	go func() {
		listener.HandleConn(conn)
		done <- true
	}()

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 3, len(packets))
	require.Equal(t, "foo:1|c\n", <-packets)
	require.Equal(t, "bar:2|g\n", <-packets)
	require.Equal(t, "baz:3|ms\n", <-packets)
}

func TestTCPEdgeCases(t *testing.T) {
	tests := []struct {
		name            string
		input           []byte
		expectedPackets []string
		expectedTrunc   float64
	}{
		{
			name:            "empty line",
			input:           []byte("foo:1|c\n\nbar:2|g\n"),
			expectedPackets: []string{"foo:1|c\n", "\n", "bar:2|g\n"},
			expectedTrunc:   0,
		},
		{
			name:            "clean close after complete line",
			input:           []byte("foo:1|c\n"),
			expectedPackets: []string{"foo:1|c\n"},
			expectedTrunc:   0,
		},
		{
			name:            "only newlines",
			input:           []byte("\n\n\n"),
			expectedPackets: []string{"\n", "\n", "\n"},
			expectedTrunc:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
			require.NoError(t, err)
			defer lc.Close()

			listener, packets, truncated := newTestTCPListener()
			listener.Conn = lc

			go sendTCPData(t, lc.Addr(), tt.input)

			conn, err := lc.AcceptTCP()
			require.NoError(t, err)

			if len(tt.expectedPackets) > 1 {
				done := make(chan bool)
				go func() {
					listener.HandleConn(conn)
					done <- true
				}()
				time.Sleep(50 * time.Millisecond)
			} else {
				listener.HandleConn(conn)
			}

			require.Equal(t, len(tt.expectedPackets), len(packets))
			for _, expected := range tt.expectedPackets {
				require.Equal(t, expected, <-packets)
			}
			require.Equal(t, tt.expectedTrunc, getCounterValue(truncated))
		})
	}
}

func TestUDPPacketHandling(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		output string
	}{
		{"without newline", []byte("foo:1|c"), "foo:1|c"},
		{"with newline", []byte("bar:2|g\n"), "bar:2|g\n"},
		{"multiple lines", []byte("foo:1|c\nbar:2|g\n"), "foo:1|c\nbar:2|g\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packets := make(chan string, 10)
			listener := &StatsDUDPListener{
				Logger:       log.NewNopLogger(),
				PacketBuffer: packets,
				UDPPackets:   prometheus.NewCounter(prometheus.CounterOpts{}),
			}

			listener.HandlePacket(tt.input)
			require.Equal(t, 1, len(packets))
			require.Equal(t, tt.output, <-packets)
		})
	}
}

func BenchmarkTCPReadLines(b *testing.B) {
	lc, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer lc.Close()

	packets := make(chan string, 1000)
	listener := &StatsDTCPListener{
		Conn:             lc,
		Logger:           log.NewNopLogger(),
		PacketBuffer:     packets,
		TCPConnections:   prometheus.NewCounter(prometheus.CounterOpts{}),
		TCPErrors:        prometheus.NewCounter(prometheus.CounterOpts{}),
		TCPLineTooLong:   prometheus.NewCounter(prometheus.CounterOpts{}),
		TruncatedPackets: prometheus.NewCounter(prometheus.CounterOpts{}),
	}

	testData := []byte("metric:1|c\n")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		go func() {
			conn, err := net.DialTCP("tcp", nil, lc.Addr().(*net.TCPAddr))
			if err != nil {
				return
			}
			defer conn.Close()
			conn.Write(testData)
		}()

		conn, err := lc.AcceptTCP()
		if err != nil {
			b.Fatal(err)
		}
		listener.HandleConn(conn)
		<-packets
	}
}
