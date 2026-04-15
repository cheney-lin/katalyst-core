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

func (pa *ProvisionAssemblerCommon) assembleDedicatedNUMAExclusiveRegion(r region.QoSRegion, result *types.InternalCPUCalculationResult) error {
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

	quotaCtrlKnobEnabled, err := metacache.IsQuotaCtrlKnobEnabled(pa.metaReader)
	if err != nil {
		return err
	}

	if quotaCtrlKnobEnabled {
		reclaimedCoresSize = available
		reclaimedCoresLimit = general.MaxFloat64(float64(reservedForReclaim), float64(available-nonReclaimRequirement))

		if quota, ok := controlKnob[configapi.ControlKnobReclaimedCoresCPUQuota]; ok {
			reclaimedCoresLimit = general.MinFloat64(reclaimedCoresLimit, quota.Value)
		}
	} else {
		reclaimedCoresSize = general.Max(reservedForReclaim, available-nonReclaimRequirement)
	}

	klog.InfoS("assembleDedicatedNUMAExclusive info", "regionName", r.Name(), "reclaimedCoresSize", reclaimedCoresSize,
		"reclaimedCoresLimit", reclaimedCoresLimit,
		"available", available, "nonReclaimRequirement", nonReclaimRequirement,
		"reservedForReclaim", reservedForReclaim, "controlKnob", controlKnob)

	// set pool overlap info for dedicated pool
	for podUID, containerSet := range r.GetPods() {
		for containerName := range containerSet {
			general.InfoS("set pool overlap pod container info",
				"poolName", commonstate.PoolNameReclaim,
				"numaID", regionNuma,
				"podUID", podUID,
				"containerName", containerName,
				"reclaimSize", reclaimedCoresSize)
			result.SetPoolOverlapPodContainerInfo(commonstate.PoolNameReclaim, regionNuma, podUID, containerName, reclaimedCoresSize)
		}
	}

	// set reclaim pool cpu limit
	result.SetPoolEntry(commonstate.PoolNameReclaim, regionNuma, 0, reclaimedCoresLimit)
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
		PoolOverlapPodContainerInfo:           map[string]map[int]map[string]map[string]int{},
		TimeStamp:                             time.Now(),
		AllowSharedCoresOverlapReclaimedCores: *pa.allowSharedCoresOverlapReclaimedCores,
	}

	pa.assembleReserve(&calculationResult)

	regionHelper := NewRegionMapHelper(*pa.regionMap)

	err := pa.assembleWithNUMABinding(regionHelper, &calculationResult)
	if err != nil {
		general.Errorf("assembleWithNUMABinding failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	err = pa.assembleWithoutNUMABinding(regionHelper, &calculationResult)
	if err != nil {
		general.Errorf("assembleWithoutNUMABinding failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	err = pa.assembleNUMABindingNUMAExclusive(regionHelper, &calculationResult)
	if err != nil {
		general.Errorf("assembleNUMABindingNUMAExclusive failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	return calculationResult, nil
}

func (pa *ProvisionAssemblerCommon) assembleWithoutNUMABinding(regionHelper *RegionMapHelper, result *types.InternalCPUCalculationResult) error {
	return pa.assembleWithoutNUMAExclusivePool(regionHelper, commonstate.FakedNUMAID, result)
}

func (pa *ProvisionAssemblerCommon) assembleWithNUMABinding(regionHelper *RegionMapHelper, result *types.InternalCPUCalculationResult) error {
	for numaID := range *pa.numaAvailable {
		err := pa.assembleWithoutNUMAExclusivePool(regionHelper, numaID, result)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pa *ProvisionAssemblerCommon) assembleNUMABindingNUMAExclusive(regionHelper *RegionMapHelper, result *types.InternalCPUCalculationResult) error {
	for numaID := range *pa.numaAvailable {
		dedicatedNUMAExclusiveRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeDedicated)
		for _, r := range dedicatedNUMAExclusiveRegions {
			if !r.IsNumaBinding() || !r.IsNumaExclusive() {
				continue
			}

			if err := pa.assembleDedicatedNUMAExclusiveRegion(r, result); err != nil {
				return fmt.Errorf("failed to assemble dedicatedNUMAExclusiveRegion: %v", err)
			}
		}
	}

	return nil
}

type poolCalculationContext struct {
	shareInfo                              regionInfo
	isolationInfo                          isolationRegionInfo
	dedicatedInfo                          regionInfo
	nodeEnableReclaim                      bool
	numaID                                 int
	reservedForReclaim                     int
	shareAndIsolatedDedicatedPoolAvailable int
	sharePoolSizeRequirements              map[string]int
	isolationPoolSizes                     map[string]int
	allowExpand                            bool
	shareAndIsolateDedicatedPoolSizes      map[string]int
	dedicatedPoolSizes                     map[string]int
	dedicatedPoolAvailable                 int
	dedicatedReclaimCoresSize              int
}

type reclaimPoolResult struct {
	reclaimedCoresSize        int
	overlapReclaimedCoresSize int
	reclaimedCoresQuota       float64
}

func (pa *ProvisionAssemblerCommon) assembleWithoutNUMAExclusivePool(
	regionHelper *RegionMapHelper,
	numaID int,
	result *types.InternalCPUCalculationResult,
) error {
	ctx, err := pa.extractAndPrepareContext(regionHelper, numaID)
	if err != nil {
		return err
	}

	if ctx == nil {
		return nil
	}

	pa.fillPoolEntries(ctx, result)

	reclaimResult, err := pa.assembleReclaimPool(ctx, result)
	if err != nil {
		return err
	}

	pa.finalizeReclaimPool(ctx, reclaimResult, result)

	return nil
}

func (pa *ProvisionAssemblerCommon) extractAndPrepareContext(
	regionHelper *RegionMapHelper,
	numaID int,
) (*poolCalculationContext, error) {
	shareRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeShare)
	shareInfo, err := extractShareRegionInfo(shareRegions)
	if err != nil {
		return nil, err
	}

	isolationRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeIsolation)
	isolationInfo, err := extractIsolationRegionInfo(isolationRegions)
	if err != nil {
		return nil, err
	}

	dedicatedRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeDedicated)
	dedicatedInfo, err := extractDedicatedRegionInfo(dedicatedRegions)
	if err != nil {
		return nil, err
	}

	if len(shareRegions) == 0 && len(isolationRegions) == 0 && len(dedicatedRegions) == 0 && numaID != commonstate.FakedNUMAID {
		return nil, nil
	}

	nodeEnableReclaim := pa.conf.GetDynamicConfiguration().EnableReclaim

	var numaSet machine.CPUSet
	if numaID == commonstate.FakedNUMAID {
		numaSet = *pa.nonBindingNumas
	} else {
		numaSet = machine.NewCPUSet(numaID)
	}

	reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, numaSet)
	shareAndIsolatedDedicatedPoolAvailable := getNUMAsResource(*pa.numaAvailable, numaSet)
	if !*pa.allowSharedCoresOverlapReclaimedCores {
		shareAndIsolatedDedicatedPoolAvailable -= reservedForReclaim
	}
	sharePoolSizeRequirements := getPoolSizeRequirements(shareInfo)

	isolationUppers := general.SumUpMapValues(isolationInfo.isolationUpperSizes)
	isolationPoolSizes := isolationInfo.isolationUpperSizes
	if general.Max(general.SumUpMapValues(shareInfo.requests), general.SumUpMapValues(shareInfo.requirements))+isolationUppers >
		shareAndIsolatedDedicatedPoolAvailable-general.SumUpMapValues(dedicatedInfo.requests) {
		isolationPoolSizes = isolationInfo.isolationLowerSizes
	}

	allowExpand := !nodeEnableReclaim || *pa.allowSharedCoresOverlapReclaimedCores
	var regulateSharePoolSizes map[string]int
	if allowExpand {
		regulateSharePoolSizes = shareInfo.requests
	} else {
		regulateSharePoolSizes = sharePoolSizeRequirements
	}
	unexpandableRequirements := general.MergeMapInt(isolationPoolSizes, dedicatedInfo.requests)
	shareAndIsolateDedicatedPoolSizes, poolThrottled := regulatePoolSizes(regulateSharePoolSizes, unexpandableRequirements, shareAndIsolatedDedicatedPoolAvailable, allowExpand)
	for _, r := range shareRegions {
		r.SetThrottled(poolThrottled)
	}

	dedicatedPoolSizes := make(map[string]int)
	for poolName := range dedicatedInfo.requests {
		if size, ok := shareAndIsolateDedicatedPoolSizes[poolName]; ok {
			dedicatedPoolSizes[poolName] = size
		}
	}
	dedicatedPoolAvailable := general.SumUpMapValues(dedicatedPoolSizes)
	dedicatedPoolSizeRequirements := getPoolSizeRequirements(dedicatedInfo)
	dedicatedReclaimCoresSize := dedicatedPoolAvailable - general.SumUpMapValues(dedicatedPoolSizeRequirements)

	general.InfoS("pool info",
		"numaID", numaID,
		"reservedForReclaim", reservedForReclaim,
		"shareRequirements", shareInfo.requirements,
		"shareRequests", shareInfo.requests,
		"shareReclaimEnable", shareInfo.reclaimEnable,
		"shareMinReclaimedCoresCPUQuota", shareInfo.minReclaimedCoresCPUQuota,
		"dedicatedRequirements", dedicatedInfo.requirements,
		"dedicatedRequests", dedicatedInfo.requests,
		"dedicatedReclaimEnable", dedicatedInfo.reclaimEnable,
		"dedicatedMinReclaimedCoresCPUQuota", dedicatedInfo.minReclaimedCoresCPUQuota,
		"dedicatedPoolAvailable", dedicatedPoolAvailable,
		"dedicatedPoolSizeRequirements", dedicatedPoolSizeRequirements,
		"dedicatedReclaimCoresSize", dedicatedReclaimCoresSize,
		"sharePoolSizeRequirements", sharePoolSizeRequirements,
		"isolationUpperSizes", isolationInfo.isolationUpperSizes,
		"isolationLowerSizes", isolationInfo.isolationLowerSizes,
		"shareAndIsolateDedicatedPoolSizes", shareAndIsolateDedicatedPoolSizes,
		"shareAndIsolatedDedicatedPoolAvailable", shareAndIsolatedDedicatedPoolAvailable)

	return &poolCalculationContext{
		shareInfo:                              shareInfo,
		isolationInfo:                          isolationInfo,
		dedicatedInfo:                          dedicatedInfo,
		nodeEnableReclaim:                      nodeEnableReclaim,
		numaID:                                 numaID,
		reservedForReclaim:                     reservedForReclaim,
		shareAndIsolatedDedicatedPoolAvailable: shareAndIsolatedDedicatedPoolAvailable,
		sharePoolSizeRequirements:              sharePoolSizeRequirements,
		isolationPoolSizes:                     isolationPoolSizes,
		allowExpand:                            allowExpand,
		shareAndIsolateDedicatedPoolSizes:      shareAndIsolateDedicatedPoolSizes,
		dedicatedPoolSizes:                     dedicatedPoolSizes,
		dedicatedPoolAvailable:                 dedicatedPoolAvailable,
		dedicatedReclaimCoresSize:              dedicatedReclaimCoresSize,
	}, nil
}

func (pa *ProvisionAssemblerCommon) fillPoolEntries(
	ctx *poolCalculationContext,
	result *types.InternalCPUCalculationResult,
) {
	for poolName, poolSize := range ctx.shareAndIsolateDedicatedPoolSizes {
		if podSet, ok := ctx.dedicatedInfo.podSet[poolName]; ok {
			for uid := range podSet {
				result.SetPoolEntry(uid, ctx.numaID, poolSize, -1)
			}
		} else {
			result.SetPoolEntry(poolName, ctx.numaID, poolSize, -1)
		}
	}
}

type overlapPoolContext struct {
	ctx    *poolCalculationContext
	result *types.InternalCPUCalculationResult
}

func (pa *ProvisionAssemblerCommon) assembleReclaimPool(ctx *poolCalculationContext, result *types.InternalCPUCalculationResult) (*reclaimPoolResult, error) {
	if *pa.allowSharedCoresOverlapReclaimedCores {
		return pa.assembleReclaimPoolWithOverlap(ctx, result)
	}
	return pa.assembleReclaimPoolWithoutOverlap(ctx, result)
}

func (pa *ProvisionAssemblerCommon) assembleReclaimPoolWithOverlap(ctx *poolCalculationContext, result *types.InternalCPUCalculationResult) (*reclaimPoolResult, error) {
	isolated := 0
	poolSizes := make(map[string]int)
	sharePoolSizes := make(map[string]int)
	reclaimablePoolSizes := make(map[string]int)
	nonReclaimableSharePoolSizes := make(map[string]int)
	reclaimableShareRequirements := make(map[string]int)
	reclaimableRequirements := make(map[string]int)

	for poolName, size := range ctx.shareAndIsolateDedicatedPoolSizes {
		if _, ok := ctx.sharePoolSizeRequirements[poolName]; ok {
			if ctx.shareInfo.reclaimEnable[poolName] {
				reclaimablePoolSizes[poolName] = size
				reclaimableShareRequirements[poolName] = ctx.shareInfo.requirements[poolName]
				reclaimableRequirements[poolName] = ctx.shareInfo.requirements[poolName]
			} else {
				nonReclaimableSharePoolSizes[poolName] = size
			}
			poolSizes[poolName] = size
			sharePoolSizes[poolName] = size
		}

		if _, ok := ctx.isolationPoolSizes[poolName]; ok {
			isolated += size
		}

		if _, ok := ctx.dedicatedInfo.requests[poolName]; ok {
			if ctx.dedicatedInfo.reclaimEnable[poolName] {
				reclaimablePoolSizes[poolName] = size
				reclaimableRequirements[poolName] = ctx.dedicatedInfo.requirements[poolName]
			}
			poolSizes[poolName] = size
		}
	}

	overlapReclaimSize := make(map[string]int)
	shareReclaimCoresSize := ctx.shareAndIsolatedDedicatedPoolAvailable - isolated -
		general.SumUpMapValues(nonReclaimableSharePoolSizes) - general.SumUpMapValues(reclaimableShareRequirements) -
		general.SumUpMapValues(ctx.dedicatedPoolSizes)

	var reclaimedCoresSize int
	if ctx.nodeEnableReclaim {
		reclaimedCoresSize = shareReclaimCoresSize + ctx.dedicatedReclaimCoresSize
		if reclaimedCoresSize < ctx.reservedForReclaim {
			reclaimedCoresSize = ctx.reservedForReclaim
			regulatedOverlapReclaimPoolSize, err := regulateOverlapReclaimPoolSize(poolSizes, reclaimedCoresSize)
			if err != nil {
				return nil, fmt.Errorf("failed to regulateOverlapReclaimPoolSize for NUMAs reserved for reclaim: %w", err)
			}
			overlapReclaimSize = regulatedOverlapReclaimPoolSize
		} else {
			for poolName, size := range reclaimablePoolSizes {
				requirement, ok := reclaimableRequirements[poolName]
				if !ok {
					continue
				}
				reclaimSize := size - requirement
				if reclaimSize > 0 {
					overlapReclaimSize[poolName] = reclaimSize
				} else {
					overlapReclaimSize[poolName] = 1
				}
			}
		}
	} else {
		reclaimedCoresSize = ctx.reservedForReclaim
		if len(poolSizes) > 0 && reclaimedCoresSize > shareReclaimCoresSize {
			reclaimedCoresSize = general.Min(reclaimedCoresSize, general.SumUpMapValues(poolSizes))
			var overlapSharePoolSizes map[string]int
			if reclaimedCoresSize <= general.SumUpMapValues(reclaimablePoolSizes) {
				overlapSharePoolSizes = reclaimablePoolSizes
			} else {
				overlapSharePoolSizes = poolSizes
			}
			reclaimSizes, err := regulateOverlapReclaimPoolSize(overlapSharePoolSizes, reclaimedCoresSize)
			if err != nil {
				return nil, fmt.Errorf("failed to regulateOverlapReclaimPoolSize: %w", err)
			}
			overlapReclaimSize = reclaimSizes
		} else if len(sharePoolSizes) > 0 && reclaimedCoresSize <= general.SumUpMapValues(sharePoolSizes) {
			reclaimSizes, err := regulateOverlapReclaimPoolSize(sharePoolSizes, reclaimedCoresSize)
			if err != nil {
				return nil, fmt.Errorf("failed to regulateOverlapReclaimPoolSize: %w", err)
			}
			overlapReclaimSize = reclaimSizes
		}
	}

	reclaimedCoresQuota := float64(-1)
	quotaCtrlKnobEnabled, err := metacache.IsQuotaCtrlKnobEnabled(pa.metaReader)
	if err != nil {
		return nil, err
	}

	if quotaCtrlKnobEnabled && ctx.numaID != commonstate.FakedNUMAID && len(poolSizes) > 0 {
		reclaimedCoresQuota = float64(general.Max(ctx.reservedForReclaim, reclaimedCoresSize))
		if ctx.shareInfo.minReclaimedCoresCPUQuota != -1 || ctx.dedicatedInfo.minReclaimedCoresCPUQuota != -1 {
			if ctx.shareInfo.minReclaimedCoresCPUQuota != -1 {
				reclaimedCoresQuota = ctx.shareInfo.minReclaimedCoresCPUQuota
			}
			if ctx.dedicatedInfo.minReclaimedCoresCPUQuota != -1 {
				reclaimedCoresQuota = general.MinFloat64(reclaimedCoresQuota, ctx.dedicatedInfo.minReclaimedCoresCPUQuota)
			}
			reclaimedCoresQuota = general.MaxFloat64(reclaimedCoresQuota, float64(ctx.reservedForReclaim))
		}
		for poolName := range overlapReclaimSize {
			overlapReclaimSize[poolName] = general.Max(overlapReclaimSize[poolName], reclaimablePoolSizes[poolName])
		}
	}

	overlapReclaimedCoresSize := 0
	for overlapPoolName, reclaimSize := range overlapReclaimSize {
		if _, ok := ctx.shareInfo.requests[overlapPoolName]; ok {
			general.InfoS("set pool overlap info",
				"poolName", commonstate.PoolNameReclaim,
				"numaID", ctx.numaID,
				"poolName", overlapPoolName,
				"reclaimSize", reclaimSize)
			result.SetPoolOverlapInfo(commonstate.PoolNameReclaim, ctx.numaID, overlapPoolName, reclaimSize)
			overlapReclaimedCoresSize += reclaimSize
			continue
		}

		if podSet, ok := ctx.dedicatedInfo.podSet[overlapPoolName]; ok {
			for podUID, containerSet := range podSet {
				for containerName := range containerSet {
					general.InfoS("set pool overlap pod container info",
						"poolName", commonstate.PoolNameReclaim,
						"numaID", ctx.numaID,
						"podUID", podUID,
						"containerName", containerName,
						"reclaimSize", reclaimSize)
					result.SetPoolOverlapPodContainerInfo(commonstate.PoolNameReclaim, ctx.numaID, podUID, containerName, reclaimSize)
				}
			}
			overlapReclaimedCoresSize += reclaimSize
			continue
		}
	}

	return &reclaimPoolResult{
		reclaimedCoresSize:        reclaimedCoresSize,
		overlapReclaimedCoresSize: overlapReclaimedCoresSize,
		reclaimedCoresQuota:       reclaimedCoresQuota,
	}, nil
}

func (pa *ProvisionAssemblerCommon) assembleReclaimPoolWithoutOverlap(ctx *poolCalculationContext, result *types.InternalCPUCalculationResult) (*reclaimPoolResult, error) {
	var reclaimedCoresSize, overlapReclaimedCoresSize int
	reclaimedCoresQuota := float64(-1)

	if ctx.nodeEnableReclaim {
		for poolName, size := range ctx.dedicatedInfo.requests {
			if ctx.dedicatedInfo.reclaimEnable[poolName] {
				reclaimSize := size - ctx.dedicatedInfo.requirements[poolName]
				if reclaimSize <= 0 {
					continue
				}
				if podSet, ok := ctx.dedicatedInfo.podSet[poolName]; ok {
					for podUID, containerSet := range podSet {
						for containerName := range containerSet {
							general.InfoS("set pool overlap pod container info",
								"poolName", commonstate.PoolNameReclaim,
								"numaID", ctx.numaID,
								"podUID", podUID,
								"containerName", containerName,
								"reclaimSize", reclaimSize)
							result.SetPoolOverlapPodContainerInfo(commonstate.PoolNameReclaim, ctx.numaID, podUID, containerName, reclaimSize)
						}
					}
					overlapReclaimedCoresSize += reclaimSize
					continue
				}
			}
		}

		shareReclaimedCoresSize := ctx.shareAndIsolatedDedicatedPoolAvailable - general.SumUpMapValues(ctx.shareAndIsolateDedicatedPoolSizes)
		reclaimedCoresSize = shareReclaimedCoresSize + ctx.dedicatedReclaimCoresSize + ctx.reservedForReclaim
	} else {
		reclaimedCoresSize = ctx.reservedForReclaim
	}

	return &reclaimPoolResult{
		reclaimedCoresSize:        reclaimedCoresSize,
		overlapReclaimedCoresSize: overlapReclaimedCoresSize,
		reclaimedCoresQuota:       reclaimedCoresQuota,
	}, nil
}

func (pa *ProvisionAssemblerCommon) finalizeReclaimPool(
	ctx *poolCalculationContext,
	reclaimResult *reclaimPoolResult,
	result *types.InternalCPUCalculationResult,
) {
	nonOverlapReclaimedCoresSize := general.Max(reclaimResult.reclaimedCoresSize-reclaimResult.overlapReclaimedCoresSize, 0)
	result.SetPoolEntry(commonstate.PoolNameReclaim, ctx.numaID, nonOverlapReclaimedCoresSize, reclaimResult.reclaimedCoresQuota)

	general.InfoS("assemble reclaim pool entry",
		"numaID", ctx.numaID,
		"reservedForReclaim", ctx.reservedForReclaim,
		"reclaimedCoresSize", reclaimResult.reclaimedCoresSize,
		"overlapReclaimedCoresSize", reclaimResult.overlapReclaimedCoresSize,
		"nonOverlapReclaimedCoresSize", nonOverlapReclaimedCoresSize,
		"reclaimedCoresQuota", reclaimResult.reclaimedCoresQuota)
}

// regionInfo is a struct that contains region information
// for share region the key of map is owner pool name
// for dedicated region the key of map is region name
type regionInfo struct {
	requirements              map[string]int
	requests                  map[string]int
	reclaimEnable             map[string]bool
	podSet                    map[string]types.PodSet
	minReclaimedCoresCPUQuota float64
}

func extractShareRegionInfo(shareRegions []region.QoSRegion) (regionInfo, error) {
	shareRequirements := make(map[string]int)
	shareRequests := make(map[string]int)
	shareReclaimEnable := make(map[string]bool)
	minReclaimedCoresCPUQuota := float64(-1)

	for _, r := range shareRegions {
		controlKnob, err := r.GetProvision()
		if err != nil {
			return regionInfo{}, err
		}
		shareRequirements[r.OwnerPoolName()] = general.Max(1, int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value))
		shareRequests[r.OwnerPoolName()] = general.Max(1, int(math.Ceil(r.GetPodsRequest())))
		shareReclaimEnable[r.OwnerPoolName()] = r.EnableReclaim()
		if quota, ok := controlKnob[configapi.ControlKnobReclaimedCoresCPUQuota]; ok {
			if minReclaimedCoresCPUQuota == -1 || quota.Value < minReclaimedCoresCPUQuota {
				minReclaimedCoresCPUQuota = quota.Value
			}
		}
	}

	return regionInfo{
		requirements:              shareRequirements,
		requests:                  shareRequests,
		reclaimEnable:             shareReclaimEnable,
		minReclaimedCoresCPUQuota: minReclaimedCoresCPUQuota,
	}, nil
}

func getPoolSizeRequirements(info regionInfo) map[string]int {
	result := make(map[string]int)
	for name, reclaimEnable := range info.reclaimEnable {
		if !reclaimEnable {
			result[name] = info.requests[name]
		} else {
			result[name] = info.requirements[name]
		}
	}
	return result
}

type isolationRegionInfo struct {
	isolationUpperSizes map[string]int
	isolationLowerSizes map[string]int
}

func extractIsolationRegionInfo(isolationRegions []region.QoSRegion) (isolationRegionInfo, error) {
	isolationUpperSizes := make(map[string]int)
	isolationLowerSizes := make(map[string]int)

	for _, r := range isolationRegions {
		controlKnob, err := r.GetProvision()
		if err != nil {
			return isolationRegionInfo{}, err
		}
		// save limits and requests for isolated region
		isolationUpperSizes[r.Name()] = int(controlKnob[configapi.ControlKnobNonIsolatedUpperCPUSize].Value)
		isolationLowerSizes[r.Name()] = int(controlKnob[configapi.ControlKnobNonIsolatedLowerCPUSize].Value)
	}

	return isolationRegionInfo{
		isolationUpperSizes: isolationUpperSizes,
		isolationLowerSizes: isolationLowerSizes,
	}, nil
}

func extractDedicatedRegionInfo(regions []region.QoSRegion) (regionInfo, error) {
	dedicatedRequirements := make(map[string]int)
	dedicatedRequests := make(map[string]int)
	dedicatedEnable := make(map[string]bool)
	dedicatedPodSet := make(map[string]types.PodSet)
	minReclaimedCoresCPUQuota := float64(-1)
	for _, r := range regions {
		if r.IsNumaExclusive() {
			continue
		}

		controlKnob, err := r.GetProvision()
		if err != nil {
			return regionInfo{}, err
		}

		regionName := r.Name()
		dedicatedRequirements[regionName] = general.Max(1, int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value))
		if r.IsNumaBinding() {
			numaBindingSize := r.GetBindingNumas().Size()
			if numaBindingSize == 0 {
				return regionInfo{}, fmt.Errorf("numa binding size is zero, region name: %s", r.Name())
			}
			dedicatedRequests[regionName] = int(math.Ceil(r.GetPodsRequest() / float64(numaBindingSize)))
		} else {
			dedicatedRequests[regionName] = int(math.Ceil(r.GetPodsRequest()))
		}
		dedicatedEnable[regionName] = r.EnableReclaim()
		dedicatedPodSet[regionName] = r.GetPods()
		if quota, ok := controlKnob[configapi.ControlKnobReclaimedCoresCPUQuota]; ok {
			if minReclaimedCoresCPUQuota == -1 || quota.Value < minReclaimedCoresCPUQuota {
				minReclaimedCoresCPUQuota = quota.Value
			}
		}
	}

	return regionInfo{
		requirements:              dedicatedRequirements,
		requests:                  dedicatedRequests,
		reclaimEnable:             dedicatedEnable,
		podSet:                    dedicatedPodSet,
		minReclaimedCoresCPUQuota: minReclaimedCoresCPUQuota,
	}, nil
}
