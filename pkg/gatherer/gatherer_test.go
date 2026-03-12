package gatherer

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type errorGatherer struct {
	families []*dto.MetricFamily
	err      error
}

func (e errorGatherer) Gather() ([]*dto.MetricFamily, error) {
	return e.families, e.err
}

func TestGatherErrorResilience(t *testing.T) {
	tests := map[string]struct {
		families      []*dto.MetricFamily
		gatherErr     error
		expectedCount int
		expectCounter bool
	}{
		"returns partial results on gather error": {
			families: []*dto.MetricFamily{
				metric("valid_counter", dto.MetricType_COUNTER),
				metric("valid_gauge", dto.MetricType_GAUGE),
			},
			gatherErr:     fmt.Errorf("collected metric foo_count was collected before with the same name and label values"),
			expectedCount: 2,
			expectCounter: true,
		},
		"succeeds with no error": {
			families: []*dto.MetricFamily{
				metric("my_counter", dto.MetricType_COUNTER),
			},
			gatherErr:     nil,
			expectedCount: 1,
			expectCounter: false,
		},
		"returns empty results on error with nil families": {
			families:      nil,
			gatherErr:     fmt.Errorf("total failure"),
			expectedCount: 0,
			expectCounter: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lookback := 30 * time.Second
			expire := 2 * time.Minute

			before := getCounterValue(t, gatherErrors)

			g := NewCounterResetGatherer(
				errorGatherer{families: tc.families, err: tc.gatherErr},
				&lookback,
				&expire,
			)

			result, err := g.Gather()
			require.NoError(t, err)
			require.Len(t, result, tc.expectedCount)

			after := getCounterValue(t, gatherErrors)
			if tc.expectCounter {
				require.Equal(t, before+1, after)
			} else {
				require.Equal(t, before, after)
			}
		})
	}
}

func getCounterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, c.Write(m))
	return m.GetCounter().GetValue()
}
