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

package provisionassembler

import (
	"fmt"
	"math"
	"time"

	"github.com/opencontainers/runc/libcontainer/cgroups"
	"k8s.io/klog/v2"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type ProvisionAssemblerCommon struct {
	conf                                  *config.Configuration
	regionMap                             *map[string]region.QoSRegion
	reservedForReclaim                    *map[int]int
	numaAvailable                         *map[int]int
	nonBindingNumas                       *machine.CPUSet
	allowSharedCoresOverlapReclaimedCores *bool

	metaReader metacache.MetaReader
	metaServer *metaserver.MetaServer
	emitter    metrics.MetricEmitter
}

func NewProvisionAssemblerCommon(conf *config.Configuration, _ interface{}, regionMap *map[string]region.QoSRegion,
	reservedForReclaim *map[int]int, numaAvailable *map[int]int, nonBindingNumas *machine.CPUSet, allowSharedCoresOverlapReclaimedCores *bool,
	metaReader metacache.MetaReader, metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter,
) ProvisionAssembler {
	return &ProvisionAssemblerCommon{
		conf:                                  conf,
		regionMap:                             regionMap,
		reservedForReclaim:                    reservedForReclaim,
		numaAvailable:                         numaAvailable,
		nonBindingNumas:                       nonBindingNumas,
		allowSharedCoresOverlapReclaimedCores: allowSharedCoresOverlapReclaimedCores,

		metaReader: metaReader,
		metaServer: metaServer,
		emitter:    emitter,
	}
}

func (pa *ProvisionAssemblerCommon) getIsolationRequirements(r region.QoSRegion) (map[string]int, error) {
	reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, r.GetBindingNumas())

	available := getNUMAsResource(*pa.numaAvailable, r.GetBindingNumas())
	if *pa.allowSharedCoresOverlapReclaimedCores {
		available += reservedForReclaim
	}

	numaID := r.GetBindingNumas().ToSliceInt()[0] // always one binding numa for this type of region

	regionHelper := NewRegionMapHelper(*pa.regionMap)
	podsRequests := general.Max(1, int(math.Ceil(r.GetPodsRequest())))

	// calc isolation pool size
	isolationRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeIsolation)

	isolationRegionControlKnobs := map[string]types.ControlKnob{}
	isolationRegionControlKnobKey := configapi.ControlKnobNonReclaimedCPURequirementUpper
	if len(isolationRegions) > 0 {
		isolationUpperSum := 0
		for _, isolationRegion := range isolationRegions {
			isolationControlKnob, err := isolationRegion.GetProvision()
			if err != nil {
				return nil, err
			}
			isolationRegionControlKnobs[isolationRegion.Name()] = isolationControlKnob
			isolationUpperSum += int(isolationControlKnob[configapi.ControlKnobNonReclaimedCPURequirementUpper].Value)
		}

		if podsRequests+isolationUpperSum > available {
			isolationRegionControlKnobKey = configapi.ControlKnobNonReclaimedCPURequirementLower
		}
	}

	isolationRequirements := map[string]int{}
	for isolationRegionName, isolationRegionControlKnob := range isolationRegionControlKnobs {
		isolationRequirements[isolationRegionName] = int(isolationRegionControlKnob[isolationRegionControlKnobKey].Value)
	}
	return isolationRequirements, nil
}

func (pa *ProvisionAssemblerCommon) assembleShareNB(r region.QoSRegion, result *types.InternalCPUCalculationResult) error {
	if r.Type() != configapi.QoSRegionTypeShare || !r.IsNumaBinding() {
		return fmt.Errorf("region %v is not a SNB region", r.Name())
	}
	controlKnob, err := r.GetProvision()
	if err != nil {
		return err
	}
	nodeEnableReclaim := pa.conf.GetDynamicConfiguration().EnableReclaim

	regionNuma := r.GetBindingNumas().ToSliceInt()[0] // always one binding numa for this type of region
	reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, r.GetBindingNumas())
	podsRequests := general.Max(1, int(math.Ceil(r.GetPodsRequest())))
	// available = NUMA Size - Reserved - ReservedForReclaimed when allowSharedCoresOverlapReclaimedCores == false
	// available = NUMA Size - Reserved when allowSharedCoresOverlapReclaimedCores == true
	available := getNUMAsResource(*pa.numaAvailable, r.GetBindingNumas())

	isolationRequirements, err := pa.getIsolationRequirements(r)
	if err != nil {
		return err
	}

	shareRequirements := map[string]int{r.OwnerPoolName(): podsRequests}

	allowExpand := !nodeEnableReclaim || *pa.allowSharedCoresOverlapReclaimedCores
	poolSizes, poolThrottled := regulatePoolSizes(shareRequirements, isolationRequirements, available, allowExpand)
	r.SetThrottled(poolThrottled)

	for poolName, size := range poolSizes {
		result.SetPoolEntry(poolName, regionNuma, size, -1)
	}

	// assemble reclaim pool
	var reclaimedCoresSize int
	reclaimedCoresLimit := float64(-1)
	if *pa.allowSharedCoresOverlapReclaimedCores {
		reclaimedCoresAvail := poolSizes[r.OwnerPoolName()] - int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value)
		if !nodeEnableReclaim {
			reclaimedCoresAvail = 0
		}
		if cgroups.IsCgroup2UnifiedMode() {
			// consider quota
			reclaimedCoresLimit = float64(general.Max(reservedForReclaim, reclaimedCoresAvail))
			if quota, ok := controlKnob[configapi.ControlKnobReclaimedCPUQuota]; ok {
				reclaimedCoresLimit = quota.Value
			}
			reclaimedCoresSize = poolSizes[r.OwnerPoolName()]
		} else {
			reclaimedCoresSize = general.Max(reservedForReclaim, reclaimedCoresAvail)
			reclaimedCoresSize = general.Min(reclaimedCoresSize, poolSizes[r.OwnerPoolName()])
		}
		result.SetPoolOverlapInfo(commonstate.PoolNameReclaim, regionNuma, r.OwnerPoolName(), reclaimedCoresSize)
	} else {
		reclaimedCoresSize = available - general.SumUpMapValues(poolSizes) + reservedForReclaim
		if !nodeEnableReclaim {
			reclaimedCoresSize = reservedForReclaim
		}
	}

	// 把这个结果放到effectiveControlKnob中，供RAMA使用
	general.InfoS("snb assemble reclaim pool entry", "reclaimedCoresSize", reclaimedCoresSize, "reclaimedCoresQuota", reclaimedCoresLimit, "regionNuma", regionNuma, "controlKnob", controlKnob)

	result.SetPoolEntry(commonstate.PoolNameReclaim, regionNuma, reclaimedCoresSize, reclaimedCoresLimit)
	return nil
}

func (pa *ProvisionAssemblerCommon) assembleIsolationNB(r region.QoSRegion, result *types.InternalCPUCalculationResult) error {
	if r.Type() != configapi.QoSRegionTypeIsolation || !r.IsNumaBinding() {
		return fmt.Errorf("region %v is not a IsolationNB region", r.Name())
	}

	controlKnob, err := r.GetProvision()
	if err != nil {
		return err
	}

	regionHelper := NewRegionMapHelper(*pa.regionMap)

	regionNuma := r.GetBindingNumas().ToSliceInt()[0] // always one binding numa for this type of region
	// If there is a SNB pool with the same NUMA ID, it will be calculated while processing the SNB pool.
	if shareRegions := regionHelper.GetRegions(regionNuma, configapi.QoSRegionTypeShare); len(shareRegions) == 0 {
		result.SetPoolEntry(r.Name(), regionNuma, int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirementUpper].Value), -1)

		_, ok := result.GetPoolEntry(commonstate.PoolNameReclaim, regionNuma)
		if !ok {
			available := getNUMAsResource(*pa.numaAvailable, r.GetBindingNumas())
			reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, r.GetBindingNumas())

			isolationRegions := regionHelper.GetRegions(regionNuma, configapi.QoSRegionTypeIsolation)
			isolationSizes := 0
			for _, ir := range isolationRegions {
				ck, err := ir.GetProvision()
				if err != nil {
					return err
				}
				isolationSizes += int(ck[configapi.ControlKnobNonReclaimedCPURequirementUpper].Value)
			}
			reclaimedCoresSize := general.Max(available-isolationSizes, reservedForReclaim)
			result.SetPoolEntry(commonstate.PoolNameReclaim, regionNuma, reclaimedCoresSize, -1)
		}
	}
	return nil
}

func (pa *ProvisionAssemblerCommon) assembleDedicatedNE(r region.QoSRegion, result *types.InternalCPUCalculationResult) error {
	if r.Type() != configapi.QoSRegionTypeDedicatedNumaExclusive {
		return fmt.Errorf("region %v is not a DedicatedNE region", r.Name())
	}

	controlKnob, err := r.GetProvision()
	if err != nil {
		return err
	}

	regionNuma := r.GetBindingNumas().ToSliceInt()[0] // always one binding numa for this type of region
	reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, r.GetBindingNumas())
	available := getNUMAsResource(*pa.numaAvailable, r.GetBindingNumas())
	var reclaimedCoresSize int
	reclaimedCoresLimit := float64(-1)

	// fill in reclaim pool entry for dedicated numa exclusive regions
	nonReclaimRequirement := int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value)
	if !r.EnableReclaim() {
		nonReclaimRequirement = available
	}
	if cgroups.IsCgroup2UnifiedMode() {
		reclaimedCoresSize = available
		reclaimedCoresLimit = general.MaxFloat64(float64(reservedForReclaim), float64(available-nonReclaimRequirement))

		if quota, ok := controlKnob[configapi.ControlKnobReclaimedCPUQuota]; ok {
			reclaimedCoresLimit = general.MinFloat64(reclaimedCoresLimit, quota.Value)
		}
	} else {
		reclaimedCoresSize = general.Max(reservedForReclaim, available-nonReclaimRequirement)
	}

	klog.InfoS("assembleDedicatedNE info", "regionName", r.Name(), "reclaimedCoresSize", reclaimedCoresSize,
		"available", available, "nonReclaimRequirement", nonReclaimRequirement,
		"reservedForReclaim", reservedForReclaim, "controlKnob", controlKnob)

	result.SetPoolEntry(commonstate.PoolNameReclaim, regionNuma, reclaimedCoresSize, reclaimedCoresLimit)
	return nil
}

func (pa *ProvisionAssemblerCommon) assembleShare(sharePoolRequirements, sharePoolRequests,
	isolationUpperSizes, isolationLowerSizes map[string]int, result *types.InternalCPUCalculationResult,
) error {
	nodeEnableReclaim := pa.conf.GetDynamicConfiguration().EnableReclaim

	isolationUppers := general.SumUpMapValues(isolationUpperSizes)

	shareAndIsolatedPoolAvailable := getNUMAsResource(*pa.numaAvailable, *pa.nonBindingNumas)
	if !*pa.allowSharedCoresOverlapReclaimedCores {
		shareAndIsolatedPoolAvailable -= getNUMAsResource(*pa.reservedForReclaim, *pa.nonBindingNumas)
	}

	isolationPoolSizes := isolationUpperSizes

	allowExpand := !nodeEnableReclaim || *pa.allowSharedCoresOverlapReclaimedCores
	var requirements map[string]int
	if allowExpand {
		requirements = sharePoolRequests
	} else {
		requirements = sharePoolRequirements
	}

	if general.SumUpMapValues(requirements)+isolationUppers > shareAndIsolatedPoolAvailable {
		isolationPoolSizes = isolationLowerSizes
	}
	shareAndIsolatePoolSizes, poolThrottled := regulatePoolSizes(requirements, isolationPoolSizes, shareAndIsolatedPoolAvailable, allowExpand)

	for _, r := range *pa.regionMap {
		if r.Type() == configapi.QoSRegionTypeShare && !r.IsNumaBinding() {
			r.SetThrottled(poolThrottled)
		}
	}

	klog.InfoS("pool info", "share pool requirement", sharePoolRequirements,
		"share pool requests", sharePoolRequests,
		"isolate upper-size", isolationUpperSizes, "isolate lower-size", isolationLowerSizes,
		"shareAndIsolatePoolSizes", shareAndIsolatePoolSizes,
		"shareAndIsolatedPoolAvailable", shareAndIsolatedPoolAvailable)

	// fill in regulated share-and-isolated pool entries
	for poolName, poolSize := range shareAndIsolatePoolSizes {
		result.SetPoolEntry(poolName, commonstate.FakedNUMAID, poolSize, -1)
	}

	// assemble reclaim pool for non-binding NUMAs
	var reclaimPoolSizeOfNonBindingNUMAs int
	nonBindingNUMAsReservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, *pa.nonBindingNumas)
	if *pa.allowSharedCoresOverlapReclaimedCores {
		isolated := 0
		sharePoolSizes := make(map[string]int)
		for poolName, size := range shareAndIsolatePoolSizes {
			_, ok := sharePoolRequirements[poolName]
			if ok {
				sharePoolSizes[poolName] = shareAndIsolatePoolSizes[poolName]
			} else {
				isolated += size
			}
		}

		reclaimPoolSizeOfNonBindingNUMAs = general.Max(nonBindingNUMAsReservedForReclaim, shareAndIsolatedPoolAvailable-isolated-general.SumUpMapValues(sharePoolRequirements))
		if !nodeEnableReclaim {
			reclaimPoolSizeOfNonBindingNUMAs = nonBindingNUMAsReservedForReclaim
		}

		if len(sharePoolSizes) > 0 {
			sharedOverlapReclaimSize := make(map[string]int)
			if !nodeEnableReclaim {
				reclaimPoolSizeOfNonBindingNUMAs = general.Min(reclaimPoolSizeOfNonBindingNUMAs, general.SumUpMapValues(sharePoolSizes))
				reclaimSizes, err := regulateOverlapReclaimPoolSize(sharePoolSizes, reclaimPoolSizeOfNonBindingNUMAs)
				if err != nil {
					return fmt.Errorf("failed to calculate sharedOverlapReclaimSize")
				}
				sharedOverlapReclaimSize = reclaimSizes
			} else {
				for poolName, size := range sharePoolSizes {
					reclaimSize := size - sharePoolRequirements[poolName]
					if reclaimSize > 0 {
						sharedOverlapReclaimSize[poolName] = reclaimSize
					} else {
						sharedOverlapReclaimSize[poolName] = 1
					}
				}

				reclaimPoolSizeOfNonBindingNUMAs = general.SumUpMapValues(sharedOverlapReclaimSize)
				if reclaimPoolSizeOfNonBindingNUMAs < nonBindingNUMAsReservedForReclaim {
					reclaimPoolSizeOfNonBindingNUMAs = nonBindingNUMAsReservedForReclaim
					regulatedOverlapReclaimPoolSize, err := regulateOverlapReclaimPoolSize(sharePoolSizes, reclaimPoolSizeOfNonBindingNUMAs)
					if err != nil {
						return fmt.Errorf("failed to regulateOverlapReclaimPoolSize for non-binding NUMAs reserved for reclaim")
					}
					sharedOverlapReclaimSize = regulatedOverlapReclaimPoolSize
				}
			}

			for overlapPoolName, size := range sharedOverlapReclaimSize {
				result.SetPoolOverlapInfo(commonstate.PoolNameReclaim, commonstate.FakedNUMAID, overlapPoolName, size)
			}
		}
	} else {
		reclaimPoolSizeOfNonBindingNUMAs = shareAndIsolatedPoolAvailable - general.SumUpMapValues(shareAndIsolatePoolSizes) + nonBindingNUMAsReservedForReclaim
		if !nodeEnableReclaim {
			reclaimPoolSizeOfNonBindingNUMAs = nonBindingNUMAsReservedForReclaim
		}
	}

	result.SetPoolEntry(commonstate.PoolNameReclaim, commonstate.FakedNUMAID, reclaimPoolSizeOfNonBindingNUMAs, -1)
	return nil
}

func (pa *ProvisionAssemblerCommon) assembleReserve(result *types.InternalCPUCalculationResult) {
	// fill in reserve pool entry
	reservePoolSize, _ := pa.metaReader.GetPoolSize(commonstate.PoolNameReserve)
	result.SetPoolEntry(commonstate.PoolNameReserve, commonstate.FakedNUMAID, reservePoolSize, -1)
}

func (pa *ProvisionAssemblerCommon) AssembleProvision() (types.InternalCPUCalculationResult, error) {
	calculationResult := types.InternalCPUCalculationResult{
		PoolEntries:                           make(map[string]map[int]types.CPUResource),
		PoolOverlapInfo:                       map[string]map[int]map[string]int{},
		TimeStamp:                             time.Now(),
		AllowSharedCoresOverlapReclaimedCores: *pa.allowSharedCoresOverlapReclaimedCores,
	}

	pa.assembleReserve(&calculationResult)

	sharePoolRequirements := make(map[string]int)
	sharePoolRequests := make(map[string]int)
	isolationUpperSizes := make(map[string]int)
	isolationLowerSizes := make(map[string]int)

	for _, r := range *pa.regionMap {
		controlKnob, err := r.GetProvision()
		if err != nil {
			return types.InternalCPUCalculationResult{}, err
		}

		// nonReclaimRequirement和reclaim quota取个小的就行
		// 如果canonical 没有返回值，rama会怎么处理？会直接使用rama的结果
		// quota 不支持multi region，因为无法控制reclaim quota在每个region的数量，reclaim cgroup是同一个

		switch r.Type() {
		case configapi.QoSRegionTypeShare:
			if r.IsNumaBinding() {
				if err := pa.assembleShareNB(r, &calculationResult); err != nil {
					return types.InternalCPUCalculationResult{}, err
				}
			} else {
				// save raw share pool sizes
				sharePoolRequirements[r.OwnerPoolName()] = general.Max(1, int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value))
				sharePoolRequests[r.OwnerPoolName()] = general.Max(1, int(math.Ceil(r.GetPodsRequest())))
			}
		case configapi.QoSRegionTypeIsolation:
			if r.IsNumaBinding() {
				if err := pa.assembleIsolationNB(r, &calculationResult); err != nil {
					return types.InternalCPUCalculationResult{}, err
				}
			} else {
				// save limits and requests for isolated region
				isolationUpperSizes[r.Name()] = int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirementUpper].Value)
				isolationLowerSizes[r.Name()] = int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirementLower].Value)
			}
		case configapi.QoSRegionTypeDedicatedNumaExclusive:
			if err := pa.assembleDedicatedNE(r, &calculationResult); err != nil {
				return types.InternalCPUCalculationResult{}, err
			}
		}
	}

	if err := pa.assembleShare(sharePoolRequirements, sharePoolRequests, isolationUpperSizes, isolationLowerSizes, &calculationResult); err != nil {
		return types.InternalCPUCalculationResult{}, err
	}

	return calculationResult, nil
}
