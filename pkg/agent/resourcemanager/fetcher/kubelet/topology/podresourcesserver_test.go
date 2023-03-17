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

package topology

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"path"
	"testing"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	podresv1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	testutil "k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state/testing"

	nodev1alpha1 "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/resourcemanager/fetcher/util/kubelet/podresources"
	"github.com/kubewharf/katalyst-core/pkg/config/generic"
	pkgconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/qos"
)

type fakePodResourcesServer struct {
	podResources         *podresv1.ListPodResourcesResponse
	allocatableResources *podresv1.AllocatableResourcesResponse
	podresv1.UnimplementedPodResourcesListerServer
}

func (m *fakePodResourcesServer) List(_ context.Context, _ *podresv1.ListPodResourcesRequest) (*podresv1.ListPodResourcesResponse, error) {
	return m.podResources, nil
}

func (m *fakePodResourcesServer) GetAllocatableResources(_ context.Context, _ *podresv1.AllocatableResourcesRequest) (*podresv1.AllocatableResourcesResponse, error) {
	return m.allocatableResources, nil
}

func newFakePodResourcesServer(podResources *podresv1.ListPodResourcesResponse, allocatableResources *podresv1.AllocatableResourcesResponse) *grpc.Server {
	server := grpc.NewServer()
	podresv1.RegisterPodResourcesListerServer(server, &fakePodResourcesServer{
		podResources:         podResources,
		allocatableResources: allocatableResources,
	})
	return server
}

type fakePodResourcesListerClient struct {
	*podresv1.ListPodResourcesResponse
	*podresv1.AllocatableResourcesResponse
}

func (f *fakePodResourcesListerClient) List(ctx context.Context, in *podresv1.ListPodResourcesRequest, opts ...grpc.CallOption) (*podresv1.ListPodResourcesResponse, error) {
	return f.ListPodResourcesResponse, nil
}

func (f *fakePodResourcesListerClient) GetAllocatableResources(ctx context.Context, in *podresv1.AllocatableResourcesRequest, opts ...grpc.CallOption) (*podresv1.AllocatableResourcesResponse, error) {
	return f.AllocatableResourcesResponse, nil
}

func generateTestPod(namespace, name, uid string, isBindNumaQoS bool) *v1.Pod {
	p := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
			Annotations: map[string]string{
				consts.PodAnnotationQoSLevelKey:          consts.PodAnnotationQoSLevelDedicatedCores,
				consts.PodAnnotationMemoryEnhancementKey: `{"numa_binding": "true"}`,
			},
		},
	}

	if isBindNumaQoS {
		p.Annotations = map[string]string{
			consts.PodAnnotationQoSLevelKey:          consts.PodAnnotationQoSLevelDedicatedCores,
			consts.PodAnnotationMemoryEnhancementKey: `{"numa_binding": "true"}`,
		}
	}
	return p
}

func testMarshal(i interface{}) string {
	marshal, err := json.Marshal(i)
	if err != nil {
		klog.Errorf("%s", err)
		return ""
	}
	return string(marshal)
}

func generateFloat64ResourceValue(value string) float64 {
	resourceValue := resource.MustParse(value)
	return float64(resourceValue.Value())
}

func tmpSocketDir() (socketDir string, err error) {
	socketDir, err = ioutil.TempDir("", "pod_resources")
	if err != nil {
		return
	}
	err = os.MkdirAll(socketDir, 0755)
	if err != nil {
		return "", err
	}
	return
}

func generateTestMetaServer(podList ...*v1.Pod) *metaserver.MetaServer {
	return &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			PodFetcher: &pod.PodFetcherStub{PodList: podList},
		},
	}
}

func Test_getNumaAllocationsByPodResources(t *testing.T) {
	type args struct {
		podList          []*v1.Pod
		podResourcesList []*podresv1.PodResources
	}
	tests := []struct {
		name    string
		args    args
		want    map[int]*nodev1alpha1.NumaStatus
		wantErr bool
	}{
		{
			name: "test-1",
			args: args{
				podList: []*v1.Pod{
					generateTestPod("default", "pod-1", "pod-1-uid", true),
					generateTestPod("default", "pod-2", "pod-2-uid", true),
					generateTestPod("default", "pod-3", "pod-3-uid", false),
				},
				podResourcesList: []*podresv1.PodResources{
					{
						Namespace: "default",
						Name:      "pod-1",
						Containers: []*podresv1.ContainerResources{
							{
								Name: "container-1",
								Devices: []*podresv1.ContainerDevices{
									{
										ResourceName: "gpu",
										Topology: &podresv1.TopologyInfo{
											Nodes: []*podresv1.NUMANode{
												{ID: 0},
											},
										},
									},
								},
								Resources: []*podresv1.TopologyAwareResource{
									{
										ResourceName: "cpu",
										OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
											{
												ResourceValue: 12,
												Node:          0,
											},
											{
												ResourceValue: 15,
												Node:          1,
											},
										},
									},
									{
										ResourceName: "memory",
										OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
											{
												ResourceValue: generateFloat64ResourceValue("12G"),
												Node:          0,
											},
											{
												ResourceValue: generateFloat64ResourceValue("15G"),
												Node:          1,
											},
										},
									},
								},
							},
						},
					},
					{
						Namespace: "default",
						Name:      "pod-2",
						Containers: []*podresv1.ContainerResources{
							{
								Name: "container-1",
								Devices: []*podresv1.ContainerDevices{
									{
										ResourceName: "gpu",
										Topology: &podresv1.TopologyInfo{
											Nodes: []*podresv1.NUMANode{
												{ID: 0},
											},
										},
									},
									{
										ResourceName: "gpu",
										Topology: &podresv1.TopologyInfo{
											Nodes: []*podresv1.NUMANode{
												{ID: 1},
											},
										},
									},
									{
										ResourceName: "disk",
									},
								},
								Resources: []*podresv1.TopologyAwareResource{
									{
										ResourceName: "cpu",
										OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
											{
												ResourceValue: 24,
												Node:          0,
											},
											{
												ResourceValue: 24,
												Node:          1,
											},
										},
									},
									{
										ResourceName: "memory",
										OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
											{
												ResourceValue: generateFloat64ResourceValue("32G"),
												Node:          0,
											},
											{
												ResourceValue: generateFloat64ResourceValue("32G"),
												Node:          1,
											},
										},
									},
								},
							},
						},
					},
					{
						Namespace: "default",
						Name:      "pod-3",
						Containers: []*podresv1.ContainerResources{
							{
								Name: "container-1",
								Devices: []*podresv1.ContainerDevices{
									{
										ResourceName: "gpu",
										Topology: &podresv1.TopologyInfo{
											Nodes: []*podresv1.NUMANode{
												{ID: 0},
											},
										},
									},
									{
										ResourceName: "gpu",
										Topology: &podresv1.TopologyInfo{
											Nodes: []*podresv1.NUMANode{
												{ID: 1},
											},
										},
									},
									{
										ResourceName: "disk",
									},
								},
								Resources: []*podresv1.TopologyAwareResource{
									{
										ResourceName: "cpu",
										OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
											{
												ResourceValue: 24,
												Node:          0,
											},
											{
												ResourceValue: 24,
												Node:          1,
											},
										},
									},
									{
										ResourceName: "memory",
										OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
											{
												ResourceValue: generateFloat64ResourceValue("32G"),
												Node:          0,
											},
											{
												ResourceValue: generateFloat64ResourceValue("32G"),
												Node:          1,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			want: map[int]*nodev1alpha1.NumaStatus{
				0: {
					NumaID: 0,
					Allocations: []*nodev1alpha1.Allocation{
						{
							Consumer: "default/pod-1/pod-1-uid",
							Requests: &v1.ResourceList{
								"gpu":    resource.MustParse("1"),
								"cpu":    resource.MustParse("12"),
								"memory": resource.MustParse("12G"),
							},
						},
						{
							Consumer: "default/pod-2/pod-2-uid",
							Requests: &v1.ResourceList{
								"gpu":    resource.MustParse("1"),
								"cpu":    resource.MustParse("24"),
								"memory": resource.MustParse("32G"),
							},
						},
						{
							Consumer: "default/pod-3/pod-3-uid",
							Requests: &v1.ResourceList{
								"gpu":    resource.MustParse("1"),
								"cpu":    resource.MustParse("24"),
								"memory": resource.MustParse("32G"),
							},
						},
					},
				},
				1: {
					NumaID: 1,
					Allocations: []*nodev1alpha1.Allocation{
						{
							Consumer: "default/pod-1/pod-1-uid",
							Requests: &v1.ResourceList{
								"cpu":    resource.MustParse("15"),
								"memory": resource.MustParse("15G"),
							},
						},
						{
							Consumer: "default/pod-2/pod-2-uid",
							Requests: &v1.ResourceList{
								"gpu":    resource.MustParse("1"),
								"cpu":    resource.MustParse("24"),
								"memory": resource.MustParse("32G"),
							},
						},
						{
							Consumer: "default/pod-3/pod-3-uid",
							Requests: &v1.ResourceList{
								"gpu":    resource.MustParse("1"),
								"cpu":    resource.MustParse("24"),
								"memory": resource.MustParse("32G"),
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qosConf := generic.NewQoSConfiguration()
			isPodNumaBinding := func(pod *v1.Pod) bool {
				return qos.IsPodNumaBinding(qosConf, pod)
			}
			got, err := getNumaAllocationsByPodResources(tt.args.podList, tt.args.podResourcesList, isPodNumaBinding)
			if (err != nil) != tt.wantErr {
				t.Errorf("getNumaAllocationsByPodResources() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !apiequality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("getNumaAllocationsByPodResources() got = %v, want %v", testMarshal(got), testMarshal(tt.want))
			}
		})
	}
}

func Test_getNumaAllocatableByAllocatableResources(t *testing.T) {
	type args struct {
		allocatableResources *podresv1.AllocatableResourcesResponse
	}
	tests := []struct {
		name                string
		args                args
		wantNumaCapacity    map[int]*v1.ResourceList
		wantNumaAllocatable map[int]*v1.ResourceList
		wantErr             bool
	}{
		{
			name: "test-1",
			args: args{
				allocatableResources: &podresv1.AllocatableResourcesResponse{
					Devices: []*podresv1.ContainerDevices{
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"0",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"1",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
					},
					Resources: []*podresv1.AllocatableTopologyAwareResource{
						{
							ResourceName: "cpu",
							TopologyAwareCapacityQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: 24,
									Node:          0,
								},
								{
									ResourceValue: 24,
									Node:          1,
								},
							},
							TopologyAwareAllocatableQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: 24,
									Node:          0,
								},
								{
									ResourceValue: 24,
									Node:          1,
								},
							},
						},
						{
							ResourceName: "memory",
							TopologyAwareCapacityQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          0,
								},
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          1,
								},
							},
							TopologyAwareAllocatableQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          0,
								},
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          1,
								},
							},
						},
					},
				},
			},
			wantNumaCapacity: map[int]*v1.ResourceList{
				0: {
					"gpu":    resource.MustParse("2"),
					"cpu":    resource.MustParse("24"),
					"memory": resource.MustParse("32G"),
				},
				1: {
					"cpu":    resource.MustParse("24"),
					"memory": resource.MustParse("32G"),
				},
			},
			wantNumaAllocatable: map[int]*v1.ResourceList{
				0: {
					"gpu":    resource.MustParse("2"),
					"cpu":    resource.MustParse("24"),
					"memory": resource.MustParse("32G"),
				},
				1: {
					"cpu":    resource.MustParse("24"),
					"memory": resource.MustParse("32G"),
				},
			},
		},
		{
			name: "test-2",
			args: args{
				allocatableResources: &podresv1.AllocatableResourcesResponse{
					Devices: []*podresv1.ContainerDevices{
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"0",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"1",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
					},
					Resources: []*podresv1.AllocatableTopologyAwareResource{
						{
							ResourceName: "cpu",
							TopologyAwareCapacityQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: 24,
									Node:          0,
								},
								{
									ResourceValue: 24,
									Node:          1,
								},
							},
							TopologyAwareAllocatableQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: 22,
									Node:          0,
								},
								{
									ResourceValue: 22,
									Node:          1,
								},
							},
						},
						{
							ResourceName: "memory",
							TopologyAwareCapacityQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          0,
								},
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          1,
								},
							},
							TopologyAwareAllocatableQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: generateFloat64ResourceValue("30G"),
									Node:          0,
								},
								{
									ResourceValue: generateFloat64ResourceValue("30G"),
									Node:          1,
								},
							},
						},
					},
				},
			},
			wantNumaCapacity: map[int]*v1.ResourceList{
				0: {
					"gpu":    resource.MustParse("2"),
					"cpu":    resource.MustParse("24"),
					"memory": resource.MustParse("32G"),
				},
				1: {
					"cpu":    resource.MustParse("24"),
					"memory": resource.MustParse("32G"),
				},
			},
			wantNumaAllocatable: map[int]*v1.ResourceList{
				0: {
					"gpu":    resource.MustParse("2"),
					"cpu":    resource.MustParse("22"),
					"memory": resource.MustParse("30G"),
				},
				1: {
					"cpu":    resource.MustParse("22"),
					"memory": resource.MustParse("30G"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			numaCapacity, numaAllocatable, err := getNumaStatusByAllocatableResources(tt.args.allocatableResources, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("getNumaStatusByAllocatableResources() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !apiequality.Semantic.DeepEqual(numaCapacity, tt.wantNumaCapacity) {
				t.Errorf("getNumaStatusByAllocatableResources() get numaCapacity = %v, wantNumaCapacity = %v",
					testMarshal(numaCapacity), testMarshal(tt.wantNumaCapacity))
			}

			if !apiequality.Semantic.DeepEqual(numaAllocatable, tt.wantNumaAllocatable) {
				t.Errorf("getNumaStatusByAllocatableResources() numaAllocatable = %v, numaAllocatable = %v",
					testMarshal(numaAllocatable), testMarshal(tt.wantNumaAllocatable))
			}
		})
	}
}

func Test_podResourcesServerTopologyAdapterImpl_GetNumaTopologyStatus(t *testing.T) {
	type fields struct {
		podList              []*v1.Pod
		listPodResources     *podresv1.ListPodResourcesResponse
		allocatableResources *podresv1.AllocatableResourcesResponse
		numaToSocketMap      map[int]int
	}
	tests := []struct {
		name    string
		fields  fields
		want    *nodev1alpha1.TopologyStatus
		wantErr bool
	}{
		{
			name: "test normal",
			fields: fields{
				podList: []*v1.Pod{
					generateTestPod("default", "pod-1", "pod-1-uid", true),
					generateTestPod("default", "pod-2", "pod-2-uid", true),
					generateTestPod("default", "pod-3", "pod-3-uid", false),
				},
				listPodResources: &podresv1.ListPodResourcesResponse{
					PodResources: []*podresv1.PodResources{
						{
							Namespace: "default",
							Name:      "pod-1",
							Containers: []*podresv1.ContainerResources{
								{
									Name: "container-1",
									Devices: []*podresv1.ContainerDevices{
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 0},
												},
											},
										},
									},
									Resources: []*podresv1.TopologyAwareResource{
										{
											ResourceName: "cpu",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: 12,
													Node:          0,
												},
												{
													ResourceValue: 15,
													Node:          1,
												},
											},
										},
										{
											ResourceName: "memory",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: generateFloat64ResourceValue("12G"),
													Node:          0,
												},
												{
													ResourceValue: generateFloat64ResourceValue("15G"),
													Node:          1,
												},
											},
										},
									},
								},
							},
						},
						{
							Namespace: "default",
							Name:      "pod-2",
							Containers: []*podresv1.ContainerResources{
								{
									Name: "container-1",
									Devices: []*podresv1.ContainerDevices{
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 0},
												},
											},
										},
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 1},
												},
											},
										},
										{
											ResourceName: "disk",
										},
									},
									Resources: []*podresv1.TopologyAwareResource{
										{
											ResourceName: "cpu",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: 24,
													Node:          0,
												},
												{
													ResourceValue: 24,
													Node:          1,
												},
											},
										},
										{
											ResourceName: "memory",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          0,
												},
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          1,
												},
											},
										},
									},
								},
							},
						},
						{
							Namespace: "default",
							Name:      "pod-3",
							Containers: []*podresv1.ContainerResources{
								{
									Name: "container-1",
									Devices: []*podresv1.ContainerDevices{
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 0},
												},
											},
										},
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 1},
												},
											},
										},
										{
											ResourceName: "disk",
										},
									},
									Resources: []*podresv1.TopologyAwareResource{
										{
											ResourceName: "cpu",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: 24,
													Node:          0,
												},
												{
													ResourceValue: 24,
													Node:          1,
												},
											},
										},
										{
											ResourceName: "memory",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          0,
												},
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          1,
												},
											},
										},
									},
								},
							},
						},
					},
				},
				allocatableResources: &podresv1.AllocatableResourcesResponse{
					Devices: []*podresv1.ContainerDevices{
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"0",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"1",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
					},
					Resources: []*podresv1.AllocatableTopologyAwareResource{
						{
							ResourceName: "cpu",
							TopologyAwareCapacityQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: 24,
									Node:          0,
								},
								{
									ResourceValue: 24,
									Node:          1,
								},
							},
							TopologyAwareAllocatableQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: 24,
									Node:          0,
								},
								{
									ResourceValue: 24,
									Node:          1,
								},
							},
						},
						{
							ResourceName: "memory",
							TopologyAwareCapacityQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          0,
								},
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          1,
								},
							},
							TopologyAwareAllocatableQuantityList: []*podresv1.TopologyAwareQuantity{
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          0,
								},
								{
									ResourceValue: generateFloat64ResourceValue("32G"),
									Node:          1,
								},
							},
						},
					},
				},
				numaToSocketMap: map[int]int{
					0: 0,
					1: 1,
				},
			},
			want: &nodev1alpha1.TopologyStatus{
				Sockets: []*nodev1alpha1.SocketStatus{
					{
						SocketID: 0,
						Numas: []*nodev1alpha1.NumaStatus{
							{
								NumaID: 0,
								Capacity: &v1.ResourceList{
									"gpu":    resource.MustParse("2"),
									"cpu":    resource.MustParse("24"),
									"memory": resource.MustParse("32G"),
								},
								Allocatable: &v1.ResourceList{
									"gpu":    resource.MustParse("2"),
									"cpu":    resource.MustParse("24"),
									"memory": resource.MustParse("32G"),
								},
								Allocations: []*nodev1alpha1.Allocation{
									{
										Consumer: "default/pod-1/pod-1-uid",
										Requests: &v1.ResourceList{
											"gpu":    resource.MustParse("1"),
											"cpu":    resource.MustParse("12"),
											"memory": resource.MustParse("12G"),
										},
									},
									{
										Consumer: "default/pod-2/pod-2-uid",
										Requests: &v1.ResourceList{
											"gpu":    resource.MustParse("1"),
											"cpu":    resource.MustParse("24"),
											"memory": resource.MustParse("32G"),
										},
									},
									{
										Consumer: "default/pod-3/pod-3-uid",
										Requests: &v1.ResourceList{
											"gpu":    resource.MustParse("1"),
											"cpu":    resource.MustParse("24"),
											"memory": resource.MustParse("32G"),
										},
									},
								},
							},
						},
					},
					{
						SocketID: 1,
						Numas: []*nodev1alpha1.NumaStatus{
							{
								NumaID: 1,
								Capacity: &v1.ResourceList{
									"cpu":    resource.MustParse("24"),
									"memory": resource.MustParse("32G"),
								},
								Allocatable: &v1.ResourceList{
									"cpu":    resource.MustParse("24"),
									"memory": resource.MustParse("32G"),
								},
								Allocations: []*nodev1alpha1.Allocation{
									{
										Consumer: "default/pod-1/pod-1-uid",
										Requests: &v1.ResourceList{
											"cpu":    resource.MustParse("15"),
											"memory": resource.MustParse("15G"),
										},
									},
									{
										Consumer: "default/pod-2/pod-2-uid",
										Requests: &v1.ResourceList{
											"gpu":    resource.MustParse("1"),
											"cpu":    resource.MustParse("24"),
											"memory": resource.MustParse("32G"),
										},
									},
									{
										Consumer: "default/pod-3/pod-3-uid",
										Requests: &v1.ResourceList{
											"gpu":    resource.MustParse("1"),
											"cpu":    resource.MustParse("24"),
											"memory": resource.MustParse("32G"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "test validation failed",
			fields: fields{
				podList: []*v1.Pod{
					generateTestPod("default", "pod-1", "pod-1-uid", true),
					generateTestPod("default", "pod-2", "pod-2-uid", true),
					generateTestPod("default", "pod-3", "pod-3-uid", false),
				},
				listPodResources: &podresv1.ListPodResourcesResponse{
					PodResources: []*podresv1.PodResources{
						{
							Namespace: "default",
							Name:      "pod-1",
							Containers: []*podresv1.ContainerResources{
								{
									Name: "container-1",
									Devices: []*podresv1.ContainerDevices{
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 0},
												},
											},
										},
									},
									Resources: []*podresv1.TopologyAwareResource{
										{
											ResourceName: "cpu",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: 12,
													Node:          0,
												},
												{
													ResourceValue: 15,
													Node:          1,
												},
											},
										},
										{
											ResourceName: "memory",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: generateFloat64ResourceValue("12G"),
													Node:          0,
												},
												{
													ResourceValue: generateFloat64ResourceValue("15G"),
													Node:          1,
												},
											},
										},
									},
								},
							},
						},
						{
							Namespace: "default",
							Name:      "pod-2",
							Containers: []*podresv1.ContainerResources{
								{
									Name: "container-1",
									Devices: []*podresv1.ContainerDevices{
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 0},
												},
											},
										},
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 1},
												},
											},
										},
										{
											ResourceName: "disk",
										},
									},
									Resources: []*podresv1.TopologyAwareResource{
										{
											ResourceName: "cpu",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: 24,
													Node:          0,
												},
												{
													ResourceValue: 24,
													Node:          1,
												},
											},
										},
										{
											ResourceName: "memory",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          0,
												},
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          1,
												},
											},
										},
									},
								},
							},
						},
						{
							Namespace: "default",
							Name:      "pod-3",
							Containers: []*podresv1.ContainerResources{
								{
									Name: "container-1",
									Devices: []*podresv1.ContainerDevices{
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 0},
												},
											},
										},
										{
											ResourceName: "gpu",
											Topology: &podresv1.TopologyInfo{
												Nodes: []*podresv1.NUMANode{
													{ID: 1},
												},
											},
										},
										{
											ResourceName: "disk",
										},
									},
									Resources: []*podresv1.TopologyAwareResource{
										{
											ResourceName: "cpu",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: 24,
													Node:          0,
												},
												{
													ResourceValue: 24,
													Node:          1,
												},
											},
										},
										{
											ResourceName: "memory",
											OriginalTopologyAwareQuantityList: []*podresv1.TopologyAwareQuantity{
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          0,
												},
												{
													ResourceValue: generateFloat64ResourceValue("32G"),
													Node:          1,
												},
											},
										},
									},
								},
							},
						},
					},
				},
				allocatableResources: &podresv1.AllocatableResourcesResponse{
					Devices: []*podresv1.ContainerDevices{
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"0",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
						{
							ResourceName: "gpu",
							DeviceIds: []string{
								"1",
							},
							Topology: &podresv1.TopologyInfo{
								Nodes: []*podresv1.NUMANode{
									{ID: 0},
								},
							},
						},
					},
					Resources: []*podresv1.AllocatableTopologyAwareResource{},
				},
				numaToSocketMap: map[int]int{
					0: 0,
					1: 1,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			p := &podResourcesServerTopologyAdapterImpl{
				client: &fakePodResourcesListerClient{
					ListPodResourcesResponse:     tt.fields.listPodResources,
					AllocatableResourcesResponse: tt.fields.allocatableResources,
				},
				metaServer: &metaserver.MetaServer{
					MetaAgent: &agent.MetaAgent{
						PodFetcher: &pod.PodFetcherStub{PodList: tt.fields.podList},
					},
				},
				numaToSocketMap: tt.fields.numaToSocketMap,
			}
			got, err := p.GetNumaTopologyStatus(context.TODO())
			if (err != nil) != tt.wantErr {
				t.Errorf("GetNumaTopologyStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, true, apiequality.Semantic.DeepEqual(tt.want, got))
		})
	}
}

func Test_podResourcesServerTopologyAdapterImpl_Run(t *testing.T) {
	dir, err := tmpSocketDir()
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	endpoints := []string{
		path.Join(dir, "podresources.sock"),
	}

	kubeletResourcePluginPath := []string{
		path.Join(dir, "resource-plugins/"),
	}

	listener, err := net.Listen("unix", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	server := newFakePodResourcesServer(
		&podresv1.ListPodResourcesResponse{},
		&podresv1.AllocatableResourcesResponse{},
	)

	go func() {
		err := server.Serve(listener)
		assert.NoError(t, err)
	}()

	testMetaServer := generateTestMetaServer()

	getNumaInfo := func() ([]info.Node, error) {
		return []info.Node{}, nil
	}

	isPodNumaBinding := func(pod *v1.Pod) bool {
		return true
	}

	ctx, cancel := context.WithCancel(context.TODO())
	notifier := make(chan struct{}, 1)
	p, _ := NewPodResourcesServerTopologyAdapter(testMetaServer,
		endpoints, kubeletResourcePluginPath,
		nil, getNumaInfo, isPodNumaBinding, podresources.GetV1Client)
	err = p.Run(ctx, func() {})
	assert.NoError(t, err)

	checkpointManager, err := checkpointmanager.NewCheckpointManager(kubeletResourcePluginPath[0])
	assert.NoError(t, err)

	err = checkpointManager.CreateCheckpoint(pkgconsts.KubeletQoSResourceManagerCheckpoint, &testutil.MockCheckpoint{})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	cancel()
	close(notifier)
	time.Sleep(1 * time.Second)
}