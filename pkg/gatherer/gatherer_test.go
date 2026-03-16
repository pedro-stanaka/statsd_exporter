package gatherer

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

var badDesc = prometheus.NewDesc("bad_metric", "triggers a gather error", nil, nil)

type badCollector struct{}

func (b badCollector) Describe(ch chan<- *prometheus.Desc) { ch <- badDesc }
func (b badCollector) Collect(ch chan<- prometheus.Metric) { ch <- badMetric{} }

type badMetric struct{}

func (b badMetric) Desc() *prometheus.Desc  { return badDesc }
func (b badMetric) Write(*dto.Metric) error { return fmt.Errorf("intentional write error") }

func TestGatherErrorResilience(t *testing.T) {
	t.Run("returns all metrics when gather succeeds", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_counter", Help: "help"})
		gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_gauge", Help: "help"})
		reg.MustRegister(counter, gauge)
		counter.Add(42)
		gauge.Set(3.14)

		g := newTestGatherer(t, reg)
		before := getCounterValue(t, g.gatherErrors)
		result, err := g.Gather()

		require.NoError(t, err)
		require.Equal(t, before, getCounterValue(t, g.gatherErrors), "error counter should not increment")
		requireCounterValue(t, result, "test_counter", 42)
		requireGaugeValue(t, result, "test_gauge", 3.14)
	})

	t.Run("drops offending metric and returns the rest on error", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "valid_counter", Help: "help"})
		gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "valid_gauge", Help: "help"})
		reg.MustRegister(counter, gauge, badCollector{})
		counter.Add(42)
		gauge.Set(3.14)

		g := newTestGatherer(t, reg)
		before := getCounterValue(t, g.gatherErrors)
		result, err := g.Gather()

		require.NoError(t, err, "gather error should be swallowed")
		require.Equal(t, before+1, getCounterValue(t, g.gatherErrors), "error counter should increment")
		requireCounterValue(t, result, "valid_counter", 42)
		requireGaugeValue(t, result, "valid_gauge", 3.14)
		require.Nil(t, findFamily(result, "bad_metric"), "offending metric should be absent")
	})

	t.Run("returns empty result when only bad collectors are registered", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		reg.MustRegister(badCollector{})

		g := newTestGatherer(t, reg)
		before := getCounterValue(t, g.gatherErrors)
		result, err := g.Gather()

		require.NoError(t, err)
		require.Equal(t, before+1, getCounterValue(t, g.gatherErrors))
		require.Empty(t, result)
	})
}

func newTestGatherer(t *testing.T, gatherer prometheus.Gatherer) *CounterResetGatherer {
	t.Helper()
	lookback := 30 * time.Second
	expire := 2 * time.Minute
	g := NewCounterResetGatherer(gatherer, &lookback, &expire)
	g.SetGatherErrors(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "statsd_exporter_gather_errors_total",
		Help: "The total number of errors encountered during metric gathering.",
	}))
	return g
}

func findFamily(families []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func requireCounterValue(t *testing.T, families []*dto.MetricFamily, name string, expected float64) {
	t.Helper()
	f := findFamily(families, name)
	require.NotNilf(t, f, "metric family %q not found", name)
	require.NotEmpty(t, f.Metric)
	require.Equal(t, expected, f.Metric[0].Counter.GetValue())
}

func requireGaugeValue(t *testing.T, families []*dto.MetricFamily, name string, expected float64) {
	t.Helper()
	f := findFamily(families, name)
	require.NotNilf(t, f, "metric family %q not found", name)
	require.NotEmpty(t, f.Metric)
	require.Equal(t, expected, f.Metric[0].Gauge.GetValue())
}

func getCounterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, c.Write(m))
	return m.GetCounter().GetValue()
}
