package filters

import (
	"context"
	"log/slog"

	"github.com/cobaltcore-dev/cortex/api/delegation/pods"
	"github.com/cobaltcore-dev/cortex/api/v1alpha1"
	"github.com/cobaltcore-dev/cortex/internal/scheduling/lib"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NoopFilter struct {
	Alias string
}

func (f *NoopFilter) Init(ctx context.Context, client client.Client, step v1alpha1.Step) error {
	return nil
}

func (NoopFilter) Run(traceLog *slog.Logger, request pods.PodPipelineRequest) (*lib.StepResult, error) {
	activations := make(map[string]float64, len(request.Nodes))
	stats := make(map[string]lib.StepStatistics)
	for _, node := range request.Nodes {
		activations[node.Name] = 1.0
	}
	return &lib.StepResult{Activations: activations, Statistics: stats}, nil
}
