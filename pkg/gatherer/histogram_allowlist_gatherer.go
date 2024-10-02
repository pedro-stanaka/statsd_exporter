package gatherer

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var SLOHistogramAllowList = []string{
	"Apps_EmailParser_parsers_parse_quality_score_bucket",
}

// HistogramAllowlistGatherer is a prometheus.Gatherer that only returns allowed histogram metrics.
type HistogramAllowlistGatherer struct {
	g prometheus.Gatherer

	allowSet map[string]struct{}
}

// NewHistogramAllowListGatherer creates a new HistogramAllowlistGatherer.
func NewHistogramAllowListGatherer(g prometheus.Gatherer, allowList []string) *HistogramAllowlistGatherer {
	allowSet := make(map[string]struct{}, len(allowList))
	for _, name := range allowList {
		name = strings.TrimSuffix(name, "_bucket")
		name = strings.TrimSuffix(name, "_sum")
		name = strings.TrimSuffix(name, "_count")
		allowSet[name] = struct{}{}
	}

	return &HistogramAllowlistGatherer{
		g:        g,
		allowSet: allowSet,
	}
}

func (n *HistogramAllowlistGatherer) Gather() ([]*dto.MetricFamily, error) {
	metrics, err := n.g.Gather()
	if err != nil {
		return nil, err
	}

	allowedMetrics := make([]*dto.MetricFamily, 0, len(metrics))
	for _, metric := range metrics {
		// Allow all non histogram metrics.
		if !isHistogram(metric) {
			allowedMetrics = append(allowedMetrics, metric)
			continue
		}

		// Allow only histogram metrics that are in the allowed set.
		if _, ok := n.allowSet[metric.GetName()]; ok {
			allowedMetrics = append(allowedMetrics, metric)
			continue
		}
	}

	return allowedMetrics, nil
}

func isHistogram(metric *dto.MetricFamily) bool {
	return metric.Type != nil && *metric.Type == dto.MetricType_HISTOGRAM
}
