// Copyright SAP SE
// SPDX-License-Identifier: Apache-2.0

package podgroupsets

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewTopology(t *testing.T) {
	tests := []struct {
		name                string
		topologyLevels      []TopologyLevelName
		nodes               []corev1.Node
		expectedLevels      int
		expectedCapacity    map[TopologyLevelName]corev1.ResourceList
		expectedAllocatable map[TopologyLevelName]corev1.ResourceList
		expectedNodes       map[TopologyLevelName]int
	}{
		{
			name:           "Empty topology with no nodes",
			topologyLevels: []TopologyLevelName{},
			nodes:          []corev1.Node{},
			expectedLevels: 1, // Only root level
			expectedCapacity: map[TopologyLevelName]corev1.ResourceList{
				TopologyRootLevel: {},
			},
			expectedAllocatable: map[TopologyLevelName]corev1.ResourceList{
				TopologyRootLevel: {},
			},
			expectedNodes: map[TopologyLevelName]int{
				TopologyRootLevel: 1, // Root node is always present
			},
		},
		{
			name:           "Empty topology with multiple nodes",
			topologyLevels: []TopologyLevelName{},
			nodes: []corev1.Node{
				createTestNode("node1", map[string]string{}, "4", "8Gi"),
				createTestNode("node2", map[string]string{}, "8", "16Gi"),
			},
			expectedLevels: 1, // Only root level
			expectedCapacity: map[TopologyLevelName]corev1.ResourceList{
				TopologyRootLevel: {
					corev1.ResourceCPU:    resource.MustParse("12"),   // 4 + 8
					corev1.ResourceMemory: resource.MustParse("24Gi"), // 8Gi + 16Gi
				},
			},
			expectedAllocatable: map[TopologyLevelName]corev1.ResourceList{
				TopologyRootLevel: {
					corev1.ResourceCPU:    resource.MustParse("12"),   // 4 + 8
					corev1.ResourceMemory: resource.MustParse("24Gi"), // 8Gi + 16Gi
				},
			},
			expectedNodes: map[TopologyLevelName]int{
				TopologyRootLevel: 1, // Root node with all nodes
			},
		},
		{
			name:           "Multi-level topology with multiple nodes",
			topologyLevels: []TopologyLevelName{"zone", "rack"},
			nodes: []corev1.Node{
				createTestNode("node1", map[string]string{
					"cortex/topology-zone": "zone-a",
					"cortex/topology-rack": "rack-1",
				}, "4", "8Gi"),
				createTestNode("node2", map[string]string{
					"cortex/topology-zone": "zone-a",
					"cortex/topology-rack": "rack-2",
				}, "4", "8Gi"),
				createTestNode("node3", map[string]string{
					"cortex/topology-zone": "zone-b",
					"cortex/topology-rack": "rack-1",
				}, "8", "16Gi"),
			},
			expectedLevels: 3, // Root + zone + rack
			expectedCapacity: map[TopologyLevelName]corev1.ResourceList{
				TopologyRootLevel: {
					corev1.ResourceCPU:    resource.MustParse("16"),   // 4 + 4 + 8
					corev1.ResourceMemory: resource.MustParse("32Gi"), // 8Gi + 8Gi + 16Gi
				},
				"zone": {
					corev1.ResourceCPU:    resource.MustParse("8"),    // zone-a: 4 + 4
					corev1.ResourceMemory: resource.MustParse("16Gi"), // zone-a: 8Gi + 8Gi
				},
				"rack": {
					corev1.ResourceCPU:    resource.MustParse("4"),   // rack-1: 4 (from node1)
					corev1.ResourceMemory: resource.MustParse("8Gi"), // rack-1: 8Gi (from node1)
				},
			},
			expectedAllocatable: map[TopologyLevelName]corev1.ResourceList{
				TopologyRootLevel: {
					corev1.ResourceCPU:    resource.MustParse("16"),   // 4 + 4 + 8
					corev1.ResourceMemory: resource.MustParse("32Gi"), // 8Gi + 8Gi + 16Gi
				},
				"zone": {
					corev1.ResourceCPU:    resource.MustParse("8"),    // zone-a: 4 + 4
					corev1.ResourceMemory: resource.MustParse("16Gi"), // zone-a: 8Gi + 8Gi
				},
				"rack": {
					corev1.ResourceCPU:    resource.MustParse("4"),   // rack-1: 4 (from node1)
					corev1.ResourceMemory: resource.MustParse("8Gi"), // rack-1: 8Gi (from node1)
				},
			},
			expectedNodes: map[TopologyLevelName]int{
				TopologyRootLevel: 1,
				"zone":            2, // zone-a, zone-b
				"rack":            3, // zone-a/rack-1, zone-a/rack-2, zone-b/rack-1
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := NewTopology(tt.topologyLevels, tt.nodes)

			// Validate basic structure
			if len(topology.Levels) != tt.expectedLevels {
				t.Errorf("Expected %d levels, got %d", tt.expectedLevels, len(topology.Levels))
			}

			// Validate first level is always root
			if topology.Levels[0] != TopologyRootLevel {
				t.Errorf("Expected first level to be %s, got %s", TopologyRootLevel, topology.Levels[0])
			}

			// Validate node counts per level
			for level, expectedCount := range tt.expectedNodes {
				if actualCount := len(topology.Nodes[level]); actualCount != expectedCount {
					t.Errorf("Level %s: expected %d nodes, got %d", level, expectedCount, actualCount)
				}
			}

			// Validate capacity and allocatable resources
			for level, expectedCapacity := range tt.expectedCapacity {
				for _, topologyNode := range topology.Nodes[level] {
					for resourceName, expectedQty := range expectedCapacity {
						actualQty := topologyNode.Capacity[resourceName]
						if !actualQty.Equal(expectedQty) {
							t.Errorf("Level %s: expected %s capacity %v, got %v", level, resourceName, expectedQty, actualQty)
						}
					}
					break // Only check the first node at each level for simplicity
				}
			}

			for level, expectedAllocatable := range tt.expectedAllocatable {
				for _, topologyNode := range topology.Nodes[level] {
					for resourceName, expectedQty := range expectedAllocatable {
						actualQty := topologyNode.Allocatable[resourceName]
						if !actualQty.Equal(expectedQty) {
							t.Errorf("Level %s: expected %s allocatable %v, got %v", level, resourceName, expectedQty, actualQty)
						}
					}
					break // Only check the first node at each level for simplicity
				}
			}
		})
	}
}

// Helper function to create test nodes
func createTestNode(name string, labels map[string]string, cpu, memory string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(memory),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(memory),
			},
		},
	}
}
