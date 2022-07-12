// Copyright 2021 The Prometheus Authors
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

package registry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/statsd_exporter/pkg/gatherer"
	"github.com/prometheus/statsd_exporter/pkg/mapper"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/lru"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/randomreplacement"
)

func TestReset(t *testing.T) {
	labels := map[string]string{
		"labelName1": "labelKey1",
		"labelName2": "labelKey2",
	}
	metricsCount := promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "statsd_exporter_metrics_total",
			Help: "The total number of metrics.",
		},
		[]string{"type"},
	)
	mapr := newTestMapperWithCache("lru", 1000)
	err := mapr.InitFromYAMLString(`---
defaults:
  match_type: regex
  observer_type: histogram
  histogram_options:
    buckets: [0.1, 0.25, 150, 500, 900]
mappings:
  - match: .+
    observer_type: histogram
    name: $0
    labels:
    original_metric_name: $0
`)
	require.NoError(t, err)
	gat := gatherer.DefaultGatherer
	reg := NewRegistry(prometheus.DefaultRegisterer, mapr, gat)
	mapping, _, _ := mapr.GetMapping("metric_name", mapper.MetricTypeCounter)

	ctr, err := reg.GetCounter("metric_name", labels, "help", mapping, metricsCount)
	require.NoError(t, err)
	ctr.Add(10)

	hst, err := reg.GetHistogram("metric_name_hist", labels, "help", mapping, metricsCount)
	require.NoError(t, err)
	hst.Observe(0.4)

	families, err := gat.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() == "metric_name" {
			require.Len(t, family.Metric, 2)
			require.Equal(t, 0.0, *family.Metric[0].Counter.Value)
			require.NotNil(t, *family.Metric[0].TimestampMs)
			require.Equal(t, 10.0, *family.Metric[1].Counter.Value)
			require.Nil(t, family.Metric[1].TimestampMs)
		}
		if family.GetName() == "metric_name_hist" {
			require.Len(t, family.Metric, 2)
			require.Len(t, family.Metric[0].Histogram.Bucket, 5)
			require.Equal(t, uint64(0), *family.Metric[0].Histogram.SampleCount)
			require.Equal(t, 0.0, *family.Metric[0].Histogram.SampleSum)
			require.NotNil(t, *family.Metric[0].TimestampMs)
			for _, bkt := range family.Metric[0].Histogram.Bucket {
				require.Equal(t, uint64(0), *bkt.CumulativeCount)
			}

			require.Len(t, family.Metric[1].Histogram.Bucket, 5)
			require.Equal(t, uint64(1), *family.Metric[1].Histogram.SampleCount)
			require.Equal(t, 0.4, *family.Metric[1].Histogram.SampleSum)
			require.Nil(t, family.Metric[1].TimestampMs)
			for _, bkt := range family.Metric[1].Histogram.Bucket {
				if *bkt.UpperBound < 150.0 {
					require.Equal(t, uint64(0), *bkt.CumulativeCount)
				} else {
					require.Equal(t, uint64(1), *bkt.CumulativeCount)
				}
			}
		}
	}
}

func newTestMapperWithCache(cacheType string, size int) *mapper.MetricMapper {
	mapr := mapper.MetricMapper{}
	var cache mapper.MetricMapperCache
	switch cacheType {
	case "lru":
		cache, _ = lru.NewMetricMapperLRUCache(mapr.Registerer, size)
	case "random":
		cache, _ = randomreplacement.NewMetricMapperRRCache(mapr.Registerer, size)
	case "none":
		return &mapr
	}
	mapr.UseCache(cache)
	return &mapr
}
