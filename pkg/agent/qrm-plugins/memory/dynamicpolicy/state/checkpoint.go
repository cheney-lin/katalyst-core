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

package state

import (
	"encoding/json"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
)

var _ checkpointmanager.Checkpoint = &MemoryPluginCheckpoint{}

type MemoryPluginCheckpoint struct {
	PolicyName         string               `json:"policyName"`
	MachineState       NUMANodeResourcesMap `json:"machineState"`
	NUMAHeadroom       map[int]int64        `json:"numa_headroom"`
	PodResourceEntries PodResourceEntries   `json:"pod_resource_entries"`
	SocketTopology     map[int]string       `json:"socket_topology,omitempty"`
	Checksum           checksum.Checksum    `json:"checksum"`
}

func NewMemoryPluginCheckpoint() *MemoryPluginCheckpoint {
	return &MemoryPluginCheckpoint{
		PodResourceEntries: make(PodResourceEntries),
		MachineState:       make(NUMANodeResourcesMap),
		SocketTopology:     make(map[int]string),
		NUMAHeadroom:       make(map[int]int64),
	}
}

// MarshalCheckpoint returns marshaled checkpoint
func (cp *MemoryPluginCheckpoint) MarshalCheckpoint() ([]byte, error) {
	// make sure checksum wasn't set before, so it doesn't affect output checksum
	cp.Checksum = 0
	cp.Checksum = checksum.New(cp)
	return json.Marshal(*cp)
}

// UnmarshalCheckpoint tries to unmarshal passed bytes to checkpoint
func (cp *MemoryPluginCheckpoint) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// VerifyChecksum verifies that current checksum of checkpoint is valid
func (cp *MemoryPluginCheckpoint) VerifyChecksum() error {
	ck := cp.Checksum
	cp.Checksum = 0
	err := ck.Verify(cp)
	cp.Checksum = ck
	return err
}
