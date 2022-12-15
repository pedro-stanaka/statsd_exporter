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
	"time"

	"github.com/go-kit/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/statsd_exporter/pkg/clock"
	"github.com/prometheus/statsd_exporter/pkg/gatherer"
	"github.com/prometheus/statsd_exporter/pkg/mapper"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/lru"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/randomreplacement"
)

type mockClock struct {
	current time.Time
}

func (m mockClock) Now() time.Time {
	return m.current
}

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
	defer reg.Registerer.Unregister(metricsCount)
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

func TestResetExpireDuration(t *testing.T) {
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
	lookBack := 30 * time.Second
	expireDuration := 2 * time.Minute

	clock.ClockInstance = &clock.Clock{
		Instant: time.Now(),
	}
	gat := gatherer.NewCounterResetGatherer(prometheus.DefaultGatherer, &lookBack, &expireDuration)
	reg := NewRegistry(prometheus.DefaultRegisterer, mapr, gat)
	defer reg.Registerer.Unregister(metricsCount)
	metricName := "test_metric_iii"
	mapping, _, _ := mapr.GetMapping(metricName, mapper.MetricTypeCounter)

	ctr, err := reg.GetCounter(metricName, labels, "help", mapping, metricsCount)
	require.NoError(t, err)
	ctr.Add(10)

	families, err := gat.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() == metricName {
			require.Len(t, family.Metric, 2)
			require.Equal(t, 0.0, *family.Metric[0].Counter.Value)
			require.NotNil(t, *family.Metric[0].TimestampMs)
			require.Equal(t, 10.0, *family.Metric[1].Counter.Value)
			require.Nil(t, family.Metric[1].TimestampMs)
		}
	}

	clock.ClockInstance.Instant = clock.ClockInstance.Instant.Add(3 * time.Minute)
	families, err = gat.Gather()
	require.NoError(t, err)

	// second gather should be same as first
	for _, family := range families {
		if family.GetName() == metricName {
			require.Len(t, family.Metric, 2)
			require.Equal(t, 0.0, *family.Metric[0].Counter.Value)
			require.NotNil(t, *family.Metric[0].TimestampMs)
			require.Equal(t, 10.0, *family.Metric[1].Counter.Value)
			require.Nil(t, family.Metric[1].TimestampMs)
		}
	}

	families, err = gat.Gather()
	require.NoError(t, err)

	// reset expired
	for _, family := range families {
		if family.GetName() == metricName {
			require.Len(t, family.Metric, 1)
			require.Equal(t, 10.0, *family.Metric[0].Counter.Value)
			require.Nil(t, family.Metric[0].TimestampMs)
		}
	}
}

func TestResetOnlyOnce(t *testing.T) {
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
	lookBack := 30 * time.Second
	expireDuration := 2 * time.Minute
	clock.ClockInstance = &clock.Clock{
		Instant: time.Now(),
	}
	gat := gatherer.NewCounterResetGatherer(prometheus.DefaultGatherer, &lookBack, &expireDuration)
	reg := NewRegistry(prometheus.DefaultRegisterer, mapr, gat)
	defer reg.Registerer.Unregister(metricsCount)
	metricName := "test_metric_xyz"
	mapping, _, _ := mapr.GetMapping(metricName, mapper.MetricTypeCounter)

	ctr, err := reg.GetCounter(metricName, labels, "help", mapping, metricsCount)
	require.NoError(t, err)
	ctr.Add(10)

	families, err := gat.Gather()
	require.NoError(t, err)

	expectedResetTimestamp := clock.Now().Add(-lookBack).UnixMilli()

	for _, family := range families {
		if family.GetName() == metricName {
			require.Len(t, family.Metric, 2)
			require.Equal(t, 0.0, *family.Metric[0].Counter.Value)
			require.Equal(t, expectedResetTimestamp, *family.Metric[0].TimestampMs)
			require.NotNil(t, *family.Metric[0].TimestampMs)
			require.Equal(t, 10.0, *family.Metric[1].Counter.Value)
			require.Nil(t, family.Metric[1].TimestampMs)
		}
	}

	ctr.Add(10)
	clock.ClockInstance.Instant = clock.ClockInstance.Instant.Add(expireDuration)
	families, err = gat.Gather()
	require.NoError(t, err)

	// second gather should be same as first
	for _, family := range families {
		if family.GetName() == metricName {
			require.Len(t, family.Metric, 2)
			require.Equal(t, 0.0, *family.Metric[0].Counter.Value)
			require.Equal(t, expectedResetTimestamp, *family.Metric[0].TimestampMs)
			require.NotNil(t, *family.Metric[0].TimestampMs)
			require.Equal(t, 20.0, *family.Metric[1].Counter.Value)
			require.Nil(t, family.Metric[1].TimestampMs)
		}
	}

	ctr.Add(10)

	families, err = gat.Gather()
	require.NoError(t, err)

	// reset expired
	for _, family := range families {
		if family.GetName() == metricName {
			require.Len(t, family.Metric, 1)
			require.Equal(t, 30.0, *family.Metric[0].Counter.Value)
			require.Nil(t, family.Metric[0].TimestampMs)
		}
	}
}

func TestResetWithTTLAndTwoSeries(t *testing.T) {
	labels := map[string]string{
		"labelName1": "labelKey1",
		"labelName2": "labelKey2",
	}
	labels2 := map[string]string{
		"labelName1": "labelKey1",
		"labelName2": "labelKeyAlternate",
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
  ttl: 3m
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
	lookBack := 30 * time.Second
	expireDuration := 2 * time.Minute
	clock.ClockInstance = &clock.Clock{
		Instant: time.Now(),
	}
	gat := gatherer.NewCounterResetGatherer(prometheus.DefaultGatherer, &lookBack, &expireDuration)
	reg := NewRegistry(prometheus.DefaultRegisterer, mapr, gat)
	defer reg.Registerer.Unregister(metricsCount)
	metricName := "test_metric_with_ttl"
	mapping, _, _ := mapr.GetMapping(metricName, mapper.MetricTypeCounter)

	ctr, err := reg.GetCounter(metricName, labels, "help", mapping, metricsCount)
	require.NoError(t, err)
	ctr2, err := reg.GetCounter(metricName, labels2, "help", mapping, metricsCount)
	require.NoError(t, err)
	ctr.Add(10)
	ctr2.Add(20)

	expectedResetTimestamp := clock.ClockInstance.Instant.Add(-lookBack).UnixMilli()
	// assuming 30s scrape and expire duration 2m, 4 scrapes should look the same
	for i := 0; i < 4; i++ {
		families, err := gat.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if family.GetName() == metricName {
				require.Len(t, family.Metric, 4)
				require.Equal(t, 0.0, *family.Metric[0].Counter.Value)
				require.Equal(t, expectedResetTimestamp, *family.Metric[0].TimestampMs)
				require.Equal(t, 0.0, *family.Metric[1].Counter.Value)
				require.Equal(t, expectedResetTimestamp, *family.Metric[1].TimestampMs)
				require.Equal(t, 10.0, *family.Metric[2].Counter.Value)
				require.Nil(t, family.Metric[2].TimestampMs)
				require.Equal(t, 20.0, *family.Metric[3].Counter.Value)
				require.Nil(t, family.Metric[3].TimestampMs)
			}
		}
		clock.ClockInstance.Instant = clock.ClockInstance.Instant.Add(30 * time.Second)
	}

	// 5 - 7th scrape reset will have expired and ttl has not kicked in
	for i := 0; i < 3; i++ {
		reg.RemoveStaleMetrics()
		families, err := gat.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if family.GetName() == metricName {
				if family.GetName() == metricName {
					require.Equal(t, 10.0, *family.Metric[0].Counter.Value)
					require.Nil(t, family.Metric[0].TimestampMs)
					require.Equal(t, 20.0, *family.Metric[1].Counter.Value)
					require.Nil(t, family.Metric[1].TimestampMs)
				}
			}
		}
		clock.ClockInstance.Instant = clock.ClockInstance.Instant.Add(30 * time.Second)
		reg.RemoveStaleMetrics()
	}

	// 8th scrape metrics ttl kicks in
	families, err := gat.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == metricName {
			if family.GetName() == metricName {
				require.Fail(t, "did not expect metric to exist")
			}
		}
	}

	// 9th scrape metrics return and reset should be back
	clock.ClockInstance.Instant = clock.ClockInstance.Instant.Add(30 * time.Second)

	ctr, err = reg.GetCounter(metricName, labels, "help", mapping, metricsCount)
	require.NoError(t, err)
	ctr2, err = reg.GetCounter(metricName, labels2, "help", mapping, metricsCount)
	require.NoError(t, err)
	ctr.Add(10)
	ctr2.Add(20)

	expectedResetTimestamp = clock.ClockInstance.Instant.Add(-lookBack).UnixMilli()
	families, err = gat.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == metricName {
			require.Len(t, family.Metric, 4)
			require.Equal(t, 0.0, *family.Metric[0].Counter.Value)
			require.Equal(t, expectedResetTimestamp, *family.Metric[0].TimestampMs)
			require.Equal(t, 0.0, *family.Metric[1].Counter.Value)
			require.Equal(t, expectedResetTimestamp, *family.Metric[1].TimestampMs)
			require.Equal(t, 10.0, *family.Metric[2].Counter.Value)
			require.Nil(t, family.Metric[2].TimestampMs)
			require.Equal(t, 20.0, *family.Metric[3].Counter.Value)
			require.Nil(t, family.Metric[3].TimestampMs)
		}
	}
}

func newTestMapperWithCache(cacheType string, size int) *mapper.MetricMapper {
	mapr := mapper.MetricMapper{
		Logger: log.NewNopLogger(),
	}
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
