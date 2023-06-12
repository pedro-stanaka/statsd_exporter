package gatherer

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type stubGatherer struct {
	families []*dto.MetricFamily
}

func (s stubGatherer) Gather() ([]*dto.MetricFamily, error) {
	return s.families, nil
}

func TestHistogramAllowListGatherer(t *testing.T) {
	families := []*dto.MetricFamily{
		metric("test_gauge", dto.MetricType_GAUGE),
		metric("test_counter", dto.MetricType_COUNTER),
		metric("allowed_histogram", dto.MetricType_HISTOGRAM),
		metric("disallowed_histogram", dto.MetricType_HISTOGRAM),
	}

	gatherer := NewHistogramAllowListGatherer(stubGatherer{families: families}, []string{"allowed_histogram"})
	result, err := gatherer.Gather()
	require.NoError(t, err)

	require.Len(t, result, 3)
	require.Equal(t, "test_gauge", *result[0].Name)
	require.Equal(t, "test_counter", *result[1].Name)
	require.Equal(t, "allowed_histogram", *result[2].Name)
}

func metric(name string, metricType dto.MetricType) *dto.MetricFamily {
	return &dto.MetricFamily{Name: &name, Type: &metricType}
}
