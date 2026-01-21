// Copyright SAP SE
// SPDX-License-Identifier: Apache-2.0

package podgroupsets

import (
	"fmt"

	"github.com/cobaltcore-dev/cortex/api/delegation/podgroupsets"
	"github.com/cobaltcore-dev/cortex/api/delegation/pods"
	"github.com/cobaltcore-dev/cortex/api/v1alpha1"
	"github.com/cobaltcore-dev/cortex/internal/scheduling/decisions/pods/helpers"
	"github.com/cobaltcore-dev/cortex/internal/scheduling/lib"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

			podRequest := pods.PodPipelineRequest{
				Nodes: nodePool,
				Pod: corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: request.PodGroupSet.Namespace,
					},
					Spec: group.Spec.PodSpec,
				},
			}

			// Run pod pipeline to find the best host for this pod
			result, err := p.PodPipeline.Run(podRequest)
			if err != nil {
				return v1alpha1.DecisionResult{}, fmt.Errorf("pod pipeline failed for pod %s: %w", podName, err)
			}
			if result.TargetHost == nil {
				return v1alpha1.DecisionResult{}, nil
			}

			nodeName := *result.TargetHost
			targetPlacements[podName] = nodeName
			placementWeight += result.AggregatedOutWeights[nodeName]

			// Simulate a reservation for the current pod
			podResourceRequests := helpers.GetPodResourceRequests(podRequest.Pod)
			for i := range nodePool {
				if nodePool[i].Name == nodeName {
					helpers.SubtractResourcesInto(nodePool[i].Status.Allocatable, podResourceRequests)
					break
				}
			}
		}
	}

	return v1alpha1.DecisionResult{
		TargetPlacements:     targetPlacements,
		AggregatedOutWeights: map[string]float64{"nodePool": placementWeight},
	}, nil
}
