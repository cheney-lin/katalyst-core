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

package region

import (
	"strconv"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/provision"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type QoSRegionDummy struct {
	*QoSRegionBase
}

func NewQoSRegionDummy(ci *types.ContainerInfo, conf *config.Configuration, extraConf interface{}, numaID int,
	metaReader metacache.MetaReader, metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter,
) QoSRegion {
	isNumaBinding := numaID != commonstate.FakedNUMAID
	r := &QoSRegionDummy{
		QoSRegionBase: NewQoSRegionBase(
			"dummy"+types.RegionNameSeparator+strconv.Itoa(numaID)+types.RegionNameSeparator+string(uuid.NewUUID()),
			"",
			"dummy",
			conf,
			extraConf,
			isNumaBinding,
			false,
			metaReader,
			metaServer,
			emitter,
		),
	}

	if isNumaBinding {
		r.cpusetMems = machine.NewCPUSet(numaID)
	}

	p := provision.NewPolicyDynamicQuota(r.name, r.regionType, r.ownerPoolName, conf, nil, metaReader, metaServer, emitter)
	r.pm.Add(p)

	return r
}

func (r *QoSRegionDummy) TryUpdateProvision() {
	r.Lock()
	defer r.Unlock()

	indicators, err := r.getIndicators()
	if err != nil {
		klog.Warningf("[qosaware-cpu] failed to get indicators, ignore it: %v", err)
	} else {
		r.ControlEssentials.Indicators = indicators
		general.Infof("indicators %v for region %v", indicators, r.name)
	}

	ctx := provision.PolicyContext{
		ResourceEssentials: r.ResourceEssentials,
		ControlEssentials:  r.ControlEssentials,
		PodSet:             r.podSet,
		CpusetMems:         r.cpusetMems,
		IsNUMABinding:      r.isNumaBinding,
		RegulatorOptions:   r.cpuRegulatorOptions,
	}
	if err := r.pm.Update(ctx); err != nil {
		general.ErrorS(err, " failed to update policy", "regionName", r.name)
	}
}
