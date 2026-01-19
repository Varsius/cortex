// Copyright SAP SE
// SPDX-License-Identifier: Apache-2.0

package podgroupsets

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cobaltcore-dev/cortex/api/delegation/podgroupsets"
	"github.com/cobaltcore-dev/cortex/api/v1alpha1"
	"github.com/cobaltcore-dev/cortex/internal/scheduling/decisions/pods"
	"github.com/cobaltcore-dev/cortex/internal/scheduling/lib"
	"github.com/cobaltcore-dev/cortex/pkg/conf"
	"github.com/cobaltcore-dev/cortex/pkg/multicluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type DecisionPipelineController struct {
	// Toolbox shared between all pipeline controllers.
	lib.BasePipelineController[lib.Pipeline[podgroupsets.PodGroupSetPipelineRequest]]

	// Mutex to only allow one process at a time
	processMu sync.Mutex

	// Config for the scheduling operator.
	Conf conf.Config

	// Monitor to pass down to all pipelines.
	Monitor lib.PipelineMonitor
}

// The type of pipeline this controller manages.
func (c *DecisionPipelineController) PipelineType() v1alpha1.PipelineType {
	return v1alpha1.PipelineTypeGang
}

func (c *DecisionPipelineController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	c.processMu.Lock()
	defer c.processMu.Unlock()

	decision := &v1alpha1.Decision{}
	if err := c.Get(ctx, req.NamespacedName, decision); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	old := decision.DeepCopy()
	if err := c.process(ctx, decision); err != nil {
		return ctrl.Result{}, err
	}
	patch := client.MergeFrom(old)
	if err := c.Status().Patch(ctx, decision, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (c *DecisionPipelineController) ProcessNewPodGroupSet(ctx context.Context, pgs *v1alpha1.PodGroupSet) error {
	c.processMu.Lock()
	defer c.processMu.Unlock()

	decision := &v1alpha1.Decision{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "podgroupset-",
		},
		Spec: v1alpha1.DecisionSpec{
			SchedulingDomain: v1alpha1.SchedulingDomainPodGroupSets,
			ResourceID:       pgs.Name,
			PipelineRef: corev1.ObjectReference{
				Name: "podgroupsets-scheduler",
			},
			PodGroupSetRef: &corev1.ObjectReference{
				Name:      pgs.Name,
				Namespace: pgs.Namespace,
			},
		},
	}

	pipelineConf, ok := c.PipelineConfigs[decision.Spec.PipelineRef.Name]
	if !ok {
		return fmt.Errorf("pipeline %s not configured", decision.Spec.PipelineRef.Name)
	}
	if pipelineConf.Spec.CreateDecisions {
		if err := c.Create(ctx, decision); err != nil {
			return err
		}
	}

	old := decision.DeepCopy()
	err := c.process(ctx, decision)
	if err != nil {
		meta.SetStatusCondition(&decision.Status.Conditions, metav1.Condition{
			Type:    v1alpha1.DecisionConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  "PipelineRunFailed",
			Message: "pipeline run failed: " + err.Error(),
		})
	} else {
		meta.SetStatusCondition(&decision.Status.Conditions, metav1.Condition{
			Type:    v1alpha1.DecisionConditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "PipelineRunSucceeded",
			Message: "pipeline run succeeded",
		})
	}
	if pipelineConf.Spec.CreateDecisions {
		patch := client.MergeFrom(old)
		if err := c.Status().Patch(ctx, decision, patch); err != nil {
			return err
		}
	}
	return err
}

func (c *DecisionPipelineController) process(ctx context.Context, decision *v1alpha1.Decision) error {
	log := ctrl.LoggerFrom(ctx)
	startedAt := time.Now()

	// TODO: implement proper lifecycle management.
	// For now, we do not touch PodSetGroups that are already running
	if decision.Status.Result != nil {
		return nil
	}

	pipeline, ok := c.Pipelines[decision.Spec.PipelineRef.Name]
	if !ok {
		log.Error(nil, "pipeline not found or not ready", "pipelineName", decision.Spec.PipelineRef.Name)
		return errors.New("pipeline not found or not ready")
	}

	// Fetch PodGroupSet
	podGroupSet := &v1alpha1.PodGroupSet{}
	if err := c.Get(ctx, client.ObjectKey{
		Name:      decision.Spec.PodGroupSetRef.Name,
		Namespace: decision.Spec.PodGroupSetRef.Namespace,
	}, podGroupSet); err != nil {
		return err
	}

	// Find nodes
	// TODO: implement inital filtering for nodes that cannot be a candidate
	// for any pod in the PodGroupSet, e.g. due to anti-affinities/taints
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return err
	}
	if len(nodes.Items) == 0 {
		return errors.New("no nodes available")
	}
	// TODO: TAS needs to be applied here.
	// For each possible node combination, the pipeline needs to evalute possible placements.
	nodePools := [][]corev1.Node{nodes.Items}
	var bestPlacements map[string]string
	var bestWeight float64
	for _, nodePool := range nodePools {
		request := podgroupsets.PodGroupSetPipelineRequest{
			PodGroupSet: *podGroupSet,
			Nodes:       nodePool,
		}
		result, err := pipeline.Run(request)
		if err != nil {
			log.V(1).Error(err, "pipeline run failed")
			return errors.New("pipeline run failed: " + err.Error())
		}

		if len(result.TargetPlacements) == 0 {
			// Gang cannot be placed on this node set
			continue
		}

		if result.AggregatedOutWeights["nodePool"] > bestWeight {
			bestPlacements = result.TargetPlacements
			bestWeight = result.AggregatedOutWeights["nodePool"]
		}
	}
	if len(bestPlacements) > 0 {
		decision.Status.Result = &v1alpha1.DecisionResult{
			TargetPlacements:     bestPlacements,
			AggregatedOutWeights: map[string]float64{"nodePool": bestWeight},
		}
	} else {
		log.Info("No valid placments found for pods in PodGroupSet", "PodGroupSet", podGroupSet.Name)
	}
	log.Info("decision processed", "duration", time.Since(startedAt))

	// Spawn pods, if valid placements have been found
	// TODO: the current approach is vulnerable to race conditions,
	// e.g. if the state of a node in Result.TargetPlacements has changed since the decision.
	// Some kind of reservation mechanism is needed to guarantee a successfull binding of all pods.
	if decision.Status.Result != nil && decision.Status.Result.TargetPlacements != nil {
		for _, group := range podGroupSet.Spec.PodGroups {
			for i := range int(group.Spec.Replicas) {
				podName := podGroupSet.PodName(group.Name, i)
				nodeName, ok := decision.Status.Result.TargetPlacements[podName]
				if !ok {
					log.Info("No placement for pod", "key", podName)
					continue
				}

				// Check if pod exists
				existing := &corev1.Pod{}
				err := c.Get(ctx, client.ObjectKey{Name: podName, Namespace: podGroupSet.Namespace}, existing)
				if err == nil {
					// exists
					continue
				} else if client.IgnoreNotFound(err) != nil {
					return err
				}

				// Create pod
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: podGroupSet.Namespace,
						OwnerReferences: []metav1.OwnerReference{
							*metav1.NewControllerRef(podGroupSet, v1alpha1.GroupVersion.WithKind("PodGroupSet")),
						},
					},
					Spec: group.Spec.PodSpec,
				}
				pod.Spec.SchedulerName = string(v1alpha1.SchedulingDomainPods)
				if err := c.Create(ctx, pod); err != nil {
					return err
				}

				// Bind
				binding := &corev1.Binding{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: podGroupSet.Namespace,
					},
					Target: corev1.ObjectReference{
						Kind: "Node",
						Name: nodeName,
					},
				}
				if err := c.Create(ctx, binding); err != nil {
					log.V(1).Error(err, "failed to assign node to pod via binding")
					return err
				}
				log.Info("created pod", "pod", podName, "node", nodeName)
			}
		}
	}

	return nil
}

func (c *DecisionPipelineController) InitPipeline(
	ctx context.Context,
	p v1alpha1.Pipeline,
) (lib.Pipeline[podgroupsets.PodGroupSetPipelineRequest], error) {
	var pipelines v1alpha1.PipelineList
	if err := c.List(ctx, &pipelines); err != nil {
		return nil, fmt.Errorf("failed to list pipelines: %w", err)
	}

	// TODO: depending on string matching for finding the pod pipeline
	// is error-prone, using some kind of reference would be preferred.
	var podPipelineConfig *v1alpha1.Pipeline
	for _, pipeline := range pipelines.Items {
		if pipeline.Spec.SchedulingDomain == v1alpha1.SchedulingDomainPods &&
			pipeline.Spec.Type == v1alpha1.PipelineTypeFilterWeigher &&
			pipeline.Name == "pods-scheduler" {
			podPipelineConfig = &pipeline
			break
		}
	}

	if podPipelineConfig == nil {
		return nil, fmt.Errorf("pod pipeline 'pods-scheduler' not found")
	}

	podPipeline, _ := lib.NewPipeline(ctx, c.Client, podPipelineConfig.Name, pods.SupportedSteps, podPipelineConfig.Spec.Steps, c.Monitor)

	return &PodGroupSetPipeline{
		PodPipeline: podPipeline,
	}, nil
}

func (c *DecisionPipelineController) handlePodGroupSet() handler.EventHandler {
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, evt event.CreateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			pgs := evt.Object.(*v1alpha1.PodGroupSet)
			if err := c.ProcessNewPodGroupSet(ctx, pgs); err != nil {
				log := ctrl.LoggerFrom(ctx)
				log.Error(err, "failed to process new pgs", "pgs", pgs.Name)
			}
		},
		UpdateFunc: func(ctx context.Context, evt event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			// TODO: Updating a PodGroupSet could become quite complicated depending on what attributes where patched.
			// Since this is not trivial, this needs further consideration and a respective design.
			/* newPodGroupSet := evt.ObjectNew.(*v1alpha1.PodGroupSet)
			if err := c.ProcessNewPodGroupSet(ctx, newPodGroupSet); err != nil {
				log := ctrl.LoggerFrom(ctx)
				log.Error(err, "failed to process updated pgs", "pgs", newPodGroupSet.Name)
			}*/
		},
		DeleteFunc: func(ctx context.Context, evt event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := ctrl.LoggerFrom(ctx)
			podgroupset := evt.Object.(*v1alpha1.PodGroupSet)
			var decisions v1alpha1.DecisionList
			if err := c.List(ctx, &decisions); err != nil {
				log.Error(err, "failed to list decisions for deleted podgroupset")
				return
			}
			// TODO: the respective pods of a PodSetGroup also need to be marked for deletion
			for _, decision := range decisions.Items {
				if decision.Spec.PodGroupSetRef != nil &&
					decision.Spec.PodGroupSetRef.Name == podgroupset.Name &&
					decision.Spec.PodGroupSetRef.Namespace == podgroupset.Namespace {
					if err := c.Delete(ctx, &decision); err != nil {
						log.Error(err, "failed to delete decision for deleted podgroupset", "decision", decision.Name)
					}
				}
			}
		},
	}
}

func (c *DecisionPipelineController) SetupWithManager(mgr manager.Manager, mcl *multicluster.Client) error {
	c.Initializer = c
	c.SchedulingDomain = v1alpha1.SchedulingDomainPodGroupSets
	if err := mgr.Add(manager.RunnableFunc(c.InitAllPipelines)); err != nil {
		return err
	}
	return multicluster.BuildController(mcl, mgr).
		WatchesMulticluster(
			&v1alpha1.PodGroupSet{},
			c.handlePodGroupSet(),
			predicate.NewPredicateFuncs(func(obj client.Object) bool {
				// We can add logic here to filter out PodGroupSets that don't need scheduling.
				return true
			}),
		).
		WatchesMulticluster(
			&v1alpha1.Pipeline{},
			handler.Funcs{
				CreateFunc: c.HandlePipelineCreated,
				UpdateFunc: c.HandlePipelineUpdated,
				DeleteFunc: c.HandlePipelineDeleted,
			},
			predicate.NewPredicateFuncs(func(obj client.Object) bool {
				pipeline := obj.(*v1alpha1.Pipeline)
				if pipeline.Spec.SchedulingDomain != v1alpha1.SchedulingDomainPodGroupSets {
					return false
				}
				return pipeline.Spec.Type == v1alpha1.PipelineTypeGang
			}),
		).
		Named("cortex-podgroupset-scheduler").
		For(
			&v1alpha1.Decision{},
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				decision := obj.(*v1alpha1.Decision)
				if decision.Spec.SchedulingDomain != v1alpha1.SchedulingDomainPodGroupSets {
					return false
				}
				// Ignore already decided schedulings.
				if decision.Status.Result != nil {
					return false
				}
				return true
			})),
		).
		Complete(c)
}
