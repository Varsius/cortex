// Copyright SAP SE
// SPDX-License-Identifier: Apache-2.0

package podgroupsets

import (
	"fmt"

	"github.com/cobaltcore-dev/cortex/api/delegation/podgroupsets"
	"github.com/cobaltcore-dev/cortex/api/delegation/pods"
	"github.com/cobaltcore-dev/cortex/api/v1alpha1"
	"github.com/cobaltcore-dev/cortex/internal/scheduling/lib"
	corev1 "k8s.io/api/core/v1"
)

type PodGroupSetPipeline struct {
	PodPipeline lib.Pipeline[pods.PodPipelineRequest]
}

func (p *PodGroupSetPipeline) Run(request podgroupsets.PodGroupSetPipelineRequest) (v1alpha1.DecisionResult, error) {
	// Copy nodes to track available capacity during gang scheduling
	nodePool := make([]corev1.Node, len(request.Nodes))
	for i, node := range request.Nodes {
		nodePool[i] = *node.DeepCopy()
	}

	targetPlacements := make(map[string]string)
	placementWeight := 0.0

	// For each pod spec, run the pod pipeline and simulate a reservation,
	// by subtracting the workload's resources from the respective node's available capacity
	for _, group := range request.PodGroupSet.Spec.PodGroups {
		for i := range int(group.Spec.Replicas) {
			podName := request.PodGroupSet.PodName(group.Name, i)

			// Run pod pipeline to find the best host for this pod
			podRequest := pods.PodPipelineRequest{
				Nodes: nodePool,
			}

			result, err := p.PodPipeline.Run(podRequest)
			if err != nil {
				return v1alpha1.DecisionResult{}, fmt.Errorf("pod pipeline failed for pod %s: %w", podName, err)
			}

			nodeName := *result.TargetHost
			targetPlacements[podName] = nodeName
			placementWeight += result.AggregatedOutWeights[nodeName]

			if err := subtractPodResourcesFromNode(&group.Spec.PodSpec, nodeName, nodePool); err != nil {
				return v1alpha1.DecisionResult{}, fmt.Errorf("failed to update node capacity for pod %s: %w", podName, err)
			}
		}
	}

	return v1alpha1.DecisionResult{
		TargetPlacements:     targetPlacements,
		AggregatedOutWeights: map[string]float64{"nodePool": placementWeight},
	}, nil
}

// subtractPodResourcesFromNode reduces the allocatable resources of a node by the pod's requirements
func subtractPodResourcesFromNode(podSpec *corev1.PodSpec, nodeName string, nodes []corev1.Node) error {
	for i := range nodes {
		if nodes[i].Name == nodeName {
			subtract(nodes[i].Status.Allocatable, podSpec.Containers)
			return nil
		}
	}
	return fmt.Errorf("node %s not found", nodeName)
}

// subtract reduces the available resources by the requests from the containers
func subtract(available corev1.ResourceList, containers []corev1.Container) {
	reqs := getRequests(containers)
	for name, qty := range reqs {
		avail := available[name]
		avail.Sub(qty)
		available[name] = avail
	}
}

// getRequests calculates the total resource requests from all containers
func getRequests(containers []corev1.Container) corev1.ResourceList {
	res := corev1.ResourceList{}
	for _, c := range containers {
		for name, qty := range c.Resources.Requests {
			if val, ok := res[name]; ok {
				val.Add(qty)
				res[name] = val
			} else {
				res[name] = qty.DeepCopy()
			}
		}
	}
	return res
}
