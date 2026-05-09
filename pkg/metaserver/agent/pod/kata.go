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

package pod

import (
	"fmt"
	"path"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/json"

	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
)

const (
	runtimeKataQemu = "kata-qemu"
	runtimeKataClh  = "kata-clh"
	runtimeKataDuma = "kata-duma"

	kataRuntimeType = "io.containerd.kata"
)

var registerOnce sync.Once

// KataContainerFetcher is responsible for fetching kata container information.
type KataContainerFetcher struct {
	runtimePodFetcher RuntimePodFetcher
}

func RegisterKataContainerFetcher(runtimePodFetcher RuntimePodFetcher) {
	kataContainerFetcher := &KataContainerFetcher{
		runtimePodFetcher: runtimePodFetcher,
	}

	registerOnce.Do(func() {
		common.RegisterAbsoluteCgroupPathHandler(common.AbsoluteCgroupPathHandler{Name: runtimeKataQemu, Handler: kataContainerFetcher.getKataContainerAbsoluteCgroupPath})
		common.RegisterAbsoluteCgroupPathHandler(common.AbsoluteCgroupPathHandler{Name: runtimeKataClh, Handler: kataContainerFetcher.getKataContainerAbsoluteCgroupPath})
		common.RegisterAbsoluteCgroupPathHandler(common.AbsoluteCgroupPathHandler{Name: runtimeKataDuma, Handler: kataContainerFetcher.getKataContainerAbsoluteCgroupPath})

		common.RegisterRelativeCgroupPathHandler(common.RelativeCgroupPathHandler{Name: runtimeKataQemu, Handler: kataContainerFetcher.getKataContainerRelativeCgroupPath})
		common.RegisterRelativeCgroupPathHandler(common.RelativeCgroupPathHandler{Name: runtimeKataClh, Handler: kataContainerFetcher.getKataContainerRelativeCgroupPath})
		common.RegisterRelativeCgroupPathHandler(common.RelativeCgroupPathHandler{Name: runtimeKataDuma, Handler: kataContainerFetcher.getKataContainerRelativeCgroupPath})
	})
}

// getKataContainerAbsoluteCgroupPath attempts to get the absolute cgroup path of a kata container
// and returns an error if it fails to do so.
func (k *KataContainerFetcher) getKataContainerAbsoluteCgroupPath(subsys string, pod *v1.Pod, containerId string) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("kata container fetcher requires non-nil pod")
	}
	podUID := string(pod.UID)
	// First check if the cgroup exists for pod level
	_, err := common.GetPodAbsCgroupPath(subsys, podUID)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for pod %s, err: %v", podUID, err)
	}
	cgroupPathSuffix, err := k.getKataCgroupPathSuffix(podUID, containerId)
	if err != nil {
		return "", fmt.Errorf("failed to get kata cgroup path suffix: %v", err)
	}
	return common.GetKubernetesAnyExistAbsCgroupPath(subsys, cgroupPathSuffix)
}

// getKataContainerRelativeCgroupPath attempts to get the relative cgroup path of a kata container
// and returns an error if it fails to do so.
func (k *KataContainerFetcher) getKataContainerRelativeCgroupPath(pod *v1.Pod, containerId string) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("kata container fetcher requires non-nil pod")
	}
	podUID := string(pod.UID)
	// First check if the cgroup exists for pod level
	_, err := common.GetPodRelativeCgroupPath(podUID)
	if err != nil {
		return "", fmt.Errorf("failed to get relative cgroup path for pod %s, err: %v", podUID, err)
	}
	cgroupPathSuffix, err := k.getKataCgroupPathSuffix(podUID, containerId)
	if err != nil {
		return "", fmt.Errorf("failed to get kata cgroup path suffix: %v", err)
	}
	return common.GetKubernetesAnyExistRelativeCgroupPath(cgroupPathSuffix)
}

// getKataCgroupPathSuffix retrieves the sandbox id of the kata container and
// then constructs the cgroup path suffix.
func (k *KataContainerFetcher) getKataCgroupPathSuffix(podUID, containerId string) (string, error) {
	infoRaw, err := k.runtimePodFetcher.GetContainerInfo(containerId)
	if err != nil {
		return "", fmt.Errorf("failed to get container info, err: %v", err)
	}

	var info ContainerInfo
	if err := json.Unmarshal([]byte(infoRaw["info"]), &info); err != nil {
		return "", fmt.Errorf("failed to unmarshal info of container into its sandbox id and runtime type, err: %v", err)
	}
	runtimeType := info.RuntimeType
	if !strings.Contains(runtimeType, kataRuntimeType) {
		return "", fmt.Errorf("runtime type of container is not kata runtime: %s", runtimeType)
	}

	sandboxId := info.SandboxID
	if sandboxId == "" {
		return "", fmt.Errorf("failed to get sandbox id of container")
	}

	kataPathSuffix := fmt.Sprintf("kata_%s", sandboxId)
	return path.Join(fmt.Sprintf("%s%s", common.PodCgroupPathPrefix, podUID), kataPathSuffix), nil
}
