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

package duma

import (
	"context"
	"path/filepath"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/global"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/metaserver"
	pkgconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/manager"
	"github.com/kubewharf/katalyst-core/pkg/util/duma"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

// NewDumaMetricsProvisioner returns the default implementation of Duma.
func NewDumaMetricsProvisioner(baseConf *global.BaseConfiguration, _ *metaserver.MetricConfiguration,
	emitter metrics.MetricEmitter, _ pod.PodFetcher, metricStore *utilmetric.MetricStore, machineInfo *machine.KatalystMachineInfo,
) types.MetricsProvisioner {
	return &DumaMetricsProvisioner{
		metricStore: metricStore,
		emitter:     emitter,
		baseConf:    baseConf,
		machineInfo: machineInfo,
	}
}

type DumaMetricsProvisioner struct {
	baseConf    *global.BaseConfiguration
	metricStore *utilmetric.MetricStore
	emitter     metrics.MetricEmitter
	machineInfo *machine.KatalystMachineInfo
}

func (m *DumaMetricsProvisioner) Run(ctx context.Context) {
	m.sample(ctx)
}

func (m *DumaMetricsProvisioner) sample(ctx context.Context) {
	klog.Infof("[duma metrics] heartbeat")

	sid := duma.GetSid()
	if sid == "" {
		klog.InfoS("no sid found, skip sample")
		return
	}

	for _, cgroupPath := range m.getReclaimedCgroupPaths() {
		absCgroupPath := filepath.Join(common.CgroupFSMountPoint, common.CgroupSubsysCPU, cgroupPath)
		cpuStat, err := manager.GetDumaManager(sid).GetCPU(absCgroupPath)
		if err != nil {
			klog.ErrorS(err, "get cpu stat failed", "cgroupPath", cgroupPath)
			continue
		}

		lastAcctUsageExisted := true
		lastAcctUsage, err := m.metricStore.GetDumaCgroupMetric(sid, cgroupPath, pkgconsts.MetricCPUAcctUsage)
		if err != nil {
			lastAcctUsageExisted = false
			klog.ErrorS(err, "get cpu acct usage failed", "cgroupPath", cgroupPath)
		}

		t := time.Now()
		klog.InfoS("sampling cpu usage", "cpuStat", cpuStat, "nano second", t.Nanosecond(), "time", t.String(),
			"cgroupPath", cgroupPath)

		m.metricStore.SetDumaCgroupMetric(sid, cgroupPath, pkgconsts.MetricCPUQuotaCgroup, utilmetric.MetricData{Value: float64(cpuStat.CpuQuota), Time: &t})
		m.metricStore.SetDumaCgroupMetric(sid, cgroupPath, pkgconsts.MetricCPUPeriodCgroup, utilmetric.MetricData{Value: float64(cpuStat.CpuPeriod), Time: &t})
		m.metricStore.SetDumaCgroupMetric(sid, cgroupPath, pkgconsts.MetricCPUAcctUsage, utilmetric.MetricData{Value: float64(cpuStat.CpuAcctUsage), Time: &t})
		if lastAcctUsageExisted {
			cpuUsage := (float64(cpuStat.CpuAcctUsage) - lastAcctUsage.Value) / float64(t.UnixNano()-lastAcctUsage.Time.UnixNano())
			m.metricStore.SetDumaCgroupMetric(sid, cgroupPath, pkgconsts.MetricCPUUsageCgroup, utilmetric.MetricData{Value: cpuUsage, Time: &t})
			klog.InfoS("sample cpu usage", "cgroupPath", cgroupPath, pkgconsts.MetricCPUUsageCgroup, cpuUsage)
		}
	}
}

func (m *DumaMetricsProvisioner) getReclaimedCgroupPaths() []string {
	cgroupPaths := []string{m.baseConf.ReclaimRelativeRootCgroupPath}
	for _, path := range common.GetNUMABindingReclaimRelativeRootCgroupPaths(m.baseConf.ReclaimRelativeRootCgroupPath,
		m.machineInfo.CPUDetails.NUMANodes().ToSliceNoSortInt()) {
		cgroupPaths = append(cgroupPaths, path)
	}
	cgroupPaths = append(cgroupPaths, m.baseConf.ReclaimRelativeRootCgroupPath)
	return cgroupPaths
}
