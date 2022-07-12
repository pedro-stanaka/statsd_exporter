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

package gatherer

import (
	"bytes"
	"hash"
	"hash/fnv"
	"net/http"
	"sort"
	"sync"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"

	"github.com/prometheus/statsd_exporter/pkg/metrics"
)

var (
	lookbackDelta   time.Duration = 30 * time.Second
	DefaultGatherer               = NewCounterResetGatherer(prometheus.DefaultGatherer, &lookbackDelta)
)

func RegisterFlags(a *kingpin.Application) {
	a.Flag("statsd.reset.lookback-delta", "The timestamp difference used for counter-resetting samples").DurationVar(&lookbackDelta)
}

func Handler() http.Handler {
	return promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer, promhttp.HandlerFor(
			DefaultGatherer,
			promhttp.HandlerOpts{},
		),
	)
}

type CounterResetGatherer struct {
	gatherer          prometheus.Gatherer
	m                 sync.Map
	ValueBuf, NameBuf bytes.Buffer
	Hasher            hash.Hash64
	lookbackDelta     *time.Duration
}

func (c *CounterResetGatherer) AddHash(metricName string, hashId uint64) {
	m, _ := c.m.LoadOrStore(metricName, &sync.Map{})
	m.(*sync.Map).Store(hashId, true)
}

func (c *CounterResetGatherer) deleteHash(metricName string, hashId uint64) {
	m, _ := c.m.Load(metricName)
	m.(*sync.Map).Delete(hashId)
}

func (c *CounterResetGatherer) FamilyExists(metricName string) bool {
	_, ok := c.m.Load(metricName)
	return ok
}

func (c *CounterResetGatherer) Exists(metricName string, hashId uint64) bool {
	m, ok := c.m.Load(metricName)
	if !ok {
		return false
	}
	_, ok = m.(*sync.Map).Load(hashId)
	return ok
}

func (c *CounterResetGatherer) Gather() ([]*dto.MetricFamily, error) {
	families, err := c.gatherer.Gather()
	if err != nil {
		return nil, err
	}
	for _, family := range families {
		if !c.FamilyExists(family.GetName()) {
			continue
		}
		resetMetrics := []*dto.Metric{}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, lbl := range metric.Label {
				labels[*lbl.Name] = *lbl.Value
			}
			hash, _ := c.HashLabels(labels)
			if c.Exists(family.GetName(), uint64(hash.Values)) && family.GetType() == dto.MetricType_COUNTER {
				resetMetrics = c.generateCounterReset(metric, resetMetrics, family, hash)
			}
			if c.Exists(family.GetName(), uint64(hash.Values)) && family.GetType() == dto.MetricType_HISTOGRAM {
				resetMetrics = c.generateHistogramReset(metric, resetMetrics, family, hash)
			}
		}
		if len(resetMetrics) > 0 {
			family.Metric = append(resetMetrics, family.Metric...)
		}
	}
	return families, nil
}

func (c *CounterResetGatherer) generateHistogramReset(
	metric *dto.Metric, resetMetrics []*dto.Metric, family *dto.MetricFamily, hash metrics.LabelHash,
) []*dto.Metric {
	zeroInt := uint64(0.0)
	zeroFloat := 0.0
	milli := time.Now().Add(-*c.lookbackDelta).UnixMilli()
	buckets := []*dto.Bucket{}
	for _, bkt := range metric.Histogram.Bucket {
		buckets = append(buckets, &dto.Bucket{
			CumulativeCount: &zeroInt,
			UpperBound:      bkt.UpperBound,
			Exemplar:        bkt.Exemplar,
		})
	}
	resetMetrics = append(resetMetrics, &dto.Metric{
		Label: metric.Label,
		Histogram: &dto.Histogram{
			SampleCount: &zeroInt,
			SampleSum:   &zeroFloat,
			Bucket:      buckets,
		},
		TimestampMs: &milli,
	})
	c.deleteHash(family.GetName(), uint64(hash.Values))
	return resetMetrics
}

func (c *CounterResetGatherer) generateCounterReset(
	metric *dto.Metric, resetMetrics []*dto.Metric, family *dto.MetricFamily, hash metrics.LabelHash,
) []*dto.Metric {
	val := 0.0
	milli := time.Now().Add(-*c.lookbackDelta).UnixMilli()
	resetMetrics = append(resetMetrics, &dto.Metric{
		Label: metric.Label,
		Counter: &dto.Counter{
			Value:    &val,
			Exemplar: metric.Counter.Exemplar,
		},
		Summary:     metric.Summary,
		TimestampMs: &milli,
	})
	c.deleteHash(family.GetName(), uint64(hash.Values))
	return resetMetrics
}

func NewCounterResetGatherer(gatherer prometheus.Gatherer, lookbackDelta *time.Duration) *CounterResetGatherer {
	return &CounterResetGatherer{
		gatherer:      gatherer,
		m:             sync.Map{},
		Hasher:        fnv.New64a(),
		lookbackDelta: lookbackDelta,
	}
}

// HashLabels Calculates a hash of both the label names and values.
func (c *CounterResetGatherer) HashLabels(labels prometheus.Labels) (metrics.LabelHash, []string) {
	c.Hasher.Reset()
	c.NameBuf.Reset()
	c.ValueBuf.Reset()
	labelNames := make([]string, 0, len(labels))

	for labelName := range labels {
		labelNames = append(labelNames, labelName)
	}
	sort.Strings(labelNames)

	c.ValueBuf.WriteByte(model.SeparatorByte)
	for _, labelName := range labelNames {
		c.ValueBuf.WriteString(labels[labelName])
		c.ValueBuf.WriteByte(model.SeparatorByte)

		c.NameBuf.WriteString(labelName)
		c.NameBuf.WriteByte(model.SeparatorByte)
	}

	lh := metrics.LabelHash{}
	c.Hasher.Write(c.NameBuf.Bytes())
	lh.Names = metrics.NameHash(c.Hasher.Sum64())

	// Now add the values to the names we've already hashed.
	c.Hasher.Write(c.ValueBuf.Bytes())
	lh.Values = metrics.ValueHash(c.Hasher.Sum64())

	return lh, labelNames
}
