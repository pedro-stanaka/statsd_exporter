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
	"fmt"
	"hash"
	"hash/fnv"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"

	"github.com/prometheus/statsd_exporter/pkg/clock"
	"github.com/prometheus/statsd_exporter/pkg/metrics"
)

const (
	scrapeAcceptHeaderText         = `application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`
	scrapeAcceptHeaderTextProtobuf = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited,application/openmetrics-text;version=1.0.0;q=0.8,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`
)

var (
	lookbackDelta             = 30 * time.Second
	expireDuration            = 2 * time.Minute
	DefaultGatherer           = NewCounterResetGatherer(prometheus.DefaultGatherer, &lookbackDelta, &expireDuration)
	HistogramGathererInstance = NewHistogramGatherer(DefaultGatherer)
)

func RegisterFlags(a *kingpin.Application) {
	a.Flag("statsd.reset.lookback-delta", "The timestamp difference used for counter-resetting samples").DurationVar(&lookbackDelta)
	a.Flag("statsd.reset.expire-duration", "Duration until counter reset samples are removed from scrape").DurationVar(&expireDuration)
}

// NativeHistogramHandler serves only histogram metrics in protobuf format.
func NativeHistogramHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Accept", scrapeAcceptHeaderTextProtobuf)
		promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(
				HistogramGathererInstance,
				promhttp.HandlerOpts{},
			),
		).ServeHTTP(w, r)
	})
}

// HandlerFloatMetrics serves float64 metrics in text format.
func HandlerFloatMetrics() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Accept", scrapeAcceptHeaderText)
		promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(
				DefaultGatherer,
				promhttp.HandlerOpts{},
			),
		).ServeHTTP(w, r)
	})
}

type CounterResetGatherer struct {
	gatherer          prometheus.Gatherer
	m                 sync.Map
	ValueBuf, NameBuf bytes.Buffer
	Hasher            hash.Hash64
	lookbackDelta     *time.Duration
	expireDuration    *time.Duration
}

func (c *CounterResetGatherer) AddHash(metricName string, hashId uint64) {
	m, _ := c.m.LoadOrStore(metricName, &sync.Map{})
	m.(*sync.Map).Store(hashId, &dto.Metric{})
}

func (c *CounterResetGatherer) deleteHash(metricName string, hashId uint64) {
	m, ok := c.m.Load(metricName)
	if !ok {
		return
	}
	resetMetric, ok := m.(*sync.Map).Load(hashId)
	if !ok || resetMetric.(*dto.Metric).TimestampMs == nil {
		return
	}
	milli := clock.Now().Add(-*c.expireDuration).UnixMilli()
	diff := clock.Now().UnixMilli() - *resetMetric.(*dto.Metric).TimestampMs
	if diff > 0 {
		//
	}
	if milli >= *resetMetric.(*dto.Metric).TimestampMs {
		m.(*sync.Map).Delete(hashId)
		return
	}
}

func (c *CounterResetGatherer) FamilyExists(metricName string) bool {
	_, ok := c.m.Load(metricName)
	return ok
}

func (c *CounterResetGatherer) LoadResetMetric(metricName string, hashId uint64) (*dto.Metric, bool) {
	m, ok := c.m.Load(metricName)
	if !ok {
		return nil, false
	}
	resetMetrics, ok := m.(*sync.Map).Load(hashId)
	if !ok {
		return nil, false
	}
	return resetMetrics.(*dto.Metric), true
}

func (c *CounterResetGatherer) updateResetMetrics(metricName string, hashId uint64, reset *dto.Metric) {
	m, ok := c.m.Load(metricName)
	if !ok {
		return
	}
	m.(*sync.Map).Store(hashId, reset)
}

func (c *CounterResetGatherer) exists(metricName string, hashId uint64) bool {
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
		// force termination, as this is non-recoverable for scraping
		fmt.Printf("failed to gather metrics: %s", err)
		os.Exit(1)
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
			if !c.exists(family.GetName(), uint64(hash.Values)) {
				continue
			}
			if family.GetType() == dto.MetricType_COUNTER {
				resetMetrics = append(resetMetrics, c.generateCounterReset(metric, family, hash))
			}
			if family.GetType() == dto.MetricType_HISTOGRAM {
				resetMetrics = append(resetMetrics, c.generateHistogramReset(metric, family, hash))
			}
		}
		if len(resetMetrics) > 0 {
			family.Metric = append(resetMetrics, family.Metric...)
		}
	}
	return families, nil
}

func (c *CounterResetGatherer) generateHistogramReset(
	metric *dto.Metric, family *dto.MetricFamily, hash metrics.LabelHash,
) *dto.Metric {
	defer c.deleteHash(family.GetName(), uint64(hash.Values))
	reset, ok := c.LoadResetMetric(family.GetName(), uint64(hash.Values))
	if ok && reset.TimestampMs != nil {
		return reset
	}

	sampleTimeMillis := clock.Now().Add(-*c.lookbackDelta).UnixMilli()
	buckets := make([]float64, 0, len(metric.Histogram.Bucket))
	labels := make(map[string]string)
	for _, lbl := range metric.Label {
		labels[lbl.GetName()] = lbl.GetValue()
	}
	for _, bkt := range metric.Histogram.Bucket {
		buckets = append(buckets, *bkt.UpperBound)
	}

	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                        family.GetName(),
		ConstLabels:                 labels,
		Buckets:                     buckets,
		NativeHistogramBucketFactor: 1.1,
	})
	resetMetric := &dto.Metric{}
	histogram.Write(resetMetric)
	resetMetric.TimestampMs = &sampleTimeMillis

	c.updateResetMetrics(family.GetName(), uint64(hash.Values), resetMetric)
	return resetMetric
}

func (c *CounterResetGatherer) generateCounterReset(
	metric *dto.Metric, family *dto.MetricFamily, hash metrics.LabelHash,
) *dto.Metric {
	defer c.deleteHash(family.GetName(), uint64(hash.Values))
	reset, ok := c.LoadResetMetric(family.GetName(), uint64(hash.Values))
	if ok && reset.TimestampMs != nil {
		return reset
	}
	val := 0.0
	milli := clock.Now().Add(-*c.lookbackDelta).UnixMilli()
	resetMetric := &dto.Metric{
		Label: metric.Label,
		Counter: &dto.Counter{
			Value:    &val,
			Exemplar: metric.Counter.Exemplar,
		},
		Summary:     metric.Summary,
		TimestampMs: &milli,
	}
	c.updateResetMetrics(family.GetName(), uint64(hash.Values), resetMetric)
	return resetMetric
}

func NewCounterResetGatherer(
	gatherer prometheus.Gatherer, lookbackDelta, expireDuration *time.Duration,
) *CounterResetGatherer {
	return &CounterResetGatherer{
		gatherer:       gatherer,
		m:              sync.Map{},
		Hasher:         fnv.New64a(),
		lookbackDelta:  lookbackDelta,
		expireDuration: expireDuration,
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
