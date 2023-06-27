/*
Copyright 2022 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Copyright 2022 The Katalyst Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"context"

	v1 "k8s.io/api/core/v1"

	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/metric"
)

// NewFakeMetricsFetcher returns a fake MetricsFetcher.
func NewFakeMetricsFetcher(emitter metrics.MetricEmitter) MetricsFetcher {
	return &FakeMetricsFetcher{
		metricStore: metric.GetMetricStoreInstance(),
		emitter:     emitter,
	}
}

type FakeMetricsFetcher struct {
	metricStore *metric.MetricStore
	emitter     metrics.MetricEmitter
}

func (f *FakeMetricsFetcher) Run(ctx context.Context) {}

func (f *FakeMetricsFetcher) RegisterNotifier(scope MetricsScope, req NotifiedRequest, response chan NotifiedResponse) string {
	return ""
}

func (f *FakeMetricsFetcher) DeRegisterNotifier(scope MetricsScope, key string) {}

func (f *FakeMetricsFetcher) GetNodeMetric(metricName string) (metric.MetricData, error) {
	return f.metricStore.GetNodeMetric(metricName)
}

func (f *FakeMetricsFetcher) GetNumaMetric(numaID int, metricName string) (metric.MetricData, error) {
	return f.metricStore.GetNumaMetric(numaID, metricName)
}

func (f *FakeMetricsFetcher) GetDeviceMetric(deviceName string, metricName string) (metric.MetricData, error) {
	return f.metricStore.GetDeviceMetric(deviceName, metricName)
}

func (f *FakeMetricsFetcher) GetCPUMetric(coreID int, metricName string) (metric.MetricData, error) {
	return f.metricStore.GetCPUMetric(coreID, metricName)
}

func (f *FakeMetricsFetcher) GetContainerMetric(podUID, containerName, metricName string) (metric.MetricData, error) {
	return f.metricStore.GetContainerMetric(podUID, containerName, metricName)
}

func (f *FakeMetricsFetcher) GetContainerNumaMetric(podUID, containerName, numaNode, metricName string) (metric.MetricData, error) {
	return f.metricStore.GetContainerNumaMetric(podUID, containerName, numaNode, metricName)
}

func (f *FakeMetricsFetcher) SetNodeMetric(metricName string, data metric.MetricData) {
	f.metricStore.SetNodeMetric(metricName, data)
}

func (f *FakeMetricsFetcher) SetNumaMetric(numaID int, metricName string, data metric.MetricData) {
	f.metricStore.SetNumaMetric(numaID, metricName, data)
}

func (f *FakeMetricsFetcher) SetCPUMetric(cpu int, metricName string, data metric.MetricData) {
	f.metricStore.SetCPUMetric(cpu, metricName, data)
}

func (f *FakeMetricsFetcher) SetDeviceMetric(deviceName string, metricName string, data metric.MetricData) {
	f.metricStore.SetDeviceMetric(deviceName, metricName, data)
}

func (f *FakeMetricsFetcher) SetContainerMetric(podUID, containerName, metricName string, data metric.MetricData) {
	f.metricStore.SetContainerMetric(podUID, containerName, metricName, data)
}

func (f *FakeMetricsFetcher) SetContainerNumaMetric(podUID, containerName, numaNode, metricName string, data metric.MetricData) {
	f.metricStore.SetContainerNumaMetric(podUID, containerName, numaNode, metricName, data)
}

func (f *FakeMetricsFetcher) AggregatePodNumaMetric(podList []*v1.Pod, numaNode, metricName string, agg metric.Aggregator, filter metric.ContainerMetricFilter) metric.MetricData {
	return f.metricStore.AggregatePodNumaMetric(podList, numaNode, metricName, agg, filter)
}

func (f *FakeMetricsFetcher) AggregatePodMetric(podList []*v1.Pod, metricName string, agg metric.Aggregator, filter metric.ContainerMetricFilter) metric.MetricData {
	return f.metricStore.AggregatePodMetric(podList, metricName, agg, filter)
}

func (f *FakeMetricsFetcher) AggregateCoreMetric(cpuset machine.CPUSet, metricName string, agg metric.Aggregator) metric.MetricData {
	return f.metricStore.AggregateCoreMetric(cpuset, metricName, agg)
}

func (f *FakeMetricsFetcher) SetQoSClassMetric(qosClass, metricName string, data metric.MetricData) {
	f.metricStore.SetQosClassMetric(qosClass, metricName, data)
}

func (f *FakeMetricsFetcher) GetQoSClassMetric(qosClass, metricName string) (metric.MetricData, error) {
	return f.metricStore.GetQosClassMetric(qosClass, metricName)
}

func (f *FakeMetricsFetcher) SetQoSClassNumaMetric(qosClass, numaNode, metricName string, data metric.MetricData) {
	f.metricStore.SetQosClassNumaMetric(qosClass, numaNode, metricName, data)
}

func (f *FakeMetricsFetcher) GetQoSClassNumaMetric(qosClass, numaNode, metricName string) (metric.MetricData, error) {
	return f.metricStore.GetQosClassNumaMetric(qosClass, numaNode, metricName)
}
