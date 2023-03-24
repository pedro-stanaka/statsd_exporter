package gatherer

import (
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// HistogramGatherer is a prometheus.Gatherer that only returns histogram metrics.
type HistogramGatherer struct {
	g      prometheus.Gatherer
	logger log.Logger
}

// NewHistogramGatherer creates a new HistogramGatherer.
func NewHistogramGatherer(g prometheus.Gatherer) *HistogramGatherer {
	return &HistogramGatherer{
		g: g,
	}
}

func (n *HistogramGatherer) Gather() ([]*dto.MetricFamily, error) {
	metrics, err := n.g.Gather()
	if err != nil {
		return nil, err
	}

	histograms := make([]*dto.MetricFamily, 0, len(metrics))
	for _, metric := range metrics {
		if metric.Type != nil && *metric.Type == dto.MetricType_HISTOGRAM {
			histograms = append(histograms, metric)
		}
	}

	return histograms, nil
}
