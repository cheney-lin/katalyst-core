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

package manager

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/duma"
)

var dumaManagers sync.Map

type dumaManager struct {
	sid string
}

func (m *dumaManager) ApplyMemory(absCgroupPath string, data *common.MemoryData) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}
	if data.LimitInBytes != 0 {
		cgroupFile := filepath.Join(absCgroupPath, "memory.limit_in_bytes")
		cmd := fmt.Sprintf("echo %d > %s", data.LimitInBytes, cgroupFile)
		_, err := duma.ExecCmd(cmd, m.sid, 30)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *dumaManager) ApplyCPU(absCgroupPath string, data *common.CPUData) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}
	if data.CpuQuota != 0 {
		cgroupFile := filepath.Join(absCgroupPath, "cpu.cfs_quota_us")
		cmd := fmt.Sprintf("echo %d > %s", data.CpuQuota, cgroupFile)
		_, err := duma.ExecCmd(cmd, m.sid, 30)
		if err != nil {
			return err
		}
	}
	if data.CpuPeriod != 0 {
		cgroupFile := filepath.Join(absCgroupPath, "cpu.cfs_period_us")
		cmd := fmt.Sprintf("echo %d > %s", data.CpuPeriod, cgroupFile)
		_, err := duma.ExecCmd(cmd, m.sid, 30)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *dumaManager) ApplyCPUSet(absCgroupPath string, data *common.CPUSetData) error {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) ApplyNetCls(absCgroupPath string, data *common.NetClsData) error {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) ApplyIOCostQoS(absCgroupPath string, devID string, data *common.IOCostQoSData) error {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) ApplyIOCostModel(absCgroupPath string, devID string, data *common.IOCostModelData) error {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) ApplyIOWeight(absCgroupPath string, devID string, weight uint64) error {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) ApplyUnifiedData(absCgroupPath, cgroupFileName, data string) error {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetMemory(absCgroupPath string) (*common.MemoryStats, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetNumaMemory(absCgroupPath string) (map[int]*common.MemoryNumaMetrics, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetMemoryPressure(absCgroupPath string, t common.PressureType) (*common.MemoryPressure, error) {
	//TODO implement me
	panic("implement me")
}

// cgv1 only
func (m *dumaManager) GetCPU(absCgroupPath string) (*common.CPUStats, error) {
	cmd := fmt.Sprintf("cat %s", filepath.Join(absCgroupPath, "cpu.cfs_period_us"))
	contents, err := duma.ExecCmd(cmd, m.sid, 10)
	if err != nil {
		return nil, err
	}
	contents = strings.TrimSpace(contents)
	cfsPeriodUs, err := strconv.ParseUint(contents, 10, 64)
	if err != nil {
		return nil, err
	}

	cmd = fmt.Sprintf("cat %s", filepath.Join(absCgroupPath, "cpu.cfs_quota_us"))
	contents, err = duma.ExecCmd(cmd, m.sid, 10)
	if err != nil {
		return nil, err
	}
	contents = strings.TrimSpace(contents)
	cfsQuotaUs, err := strconv.ParseInt(contents, 10, 64)
	if err != nil {
		return nil, err
	}

	cmd = fmt.Sprintf("cat %s", filepath.Join(absCgroupPath, "cpuacct.usage"))
	contents, err = duma.ExecCmd(cmd, m.sid, 10)
	if err != nil {
		return nil, err
	}
	contents = strings.TrimSpace(contents)
	cpuAcctUsage, err := strconv.ParseUint(contents, 10, 64)
	if err != nil {
		return nil, err
	}

	return &common.CPUStats{CpuPeriod: cfsPeriodUs, CpuQuota: cfsQuotaUs, CpuAcctUsage: cpuAcctUsage}, nil
}

func (m *dumaManager) GetCPUSet(absCgroupPath string) (*common.CPUSetStats, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetIOCostQoS(absCgroupPath string) (map[string]*common.IOCostQoSData, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetIOCostModel(absCgroupPath string) (map[string]*common.IOCostModelData, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetDeviceIOWeight(absCgroupPath string, devID string) (uint64, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetIOStat(absCgroupPath string) (map[string]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetMetrics(relCgroupPath string, subsystems map[string]struct{}) (*common.CgroupMetrics, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetPids(absCgroupPath string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dumaManager) GetTasks(absCgroupPath string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

// GetDumaManager returns a cgroup instance for both v1/v2 version
func GetDumaManager(sid string) Manager {
	m, ok := dumaManagers.Load(sid)
	if ok {
		return m.(*dumaManager)
	}

	m = &dumaManager{sid: sid}
	dumaManagers.Store(sid, m)
	return m.(*dumaManager)
}
