//go:build e2e

package e2e

import (
	"context"
	"fmt"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/engine"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// tigeraStatusReader informs `helm install --wait` when Tigera resources are
// ready. Helm's built-in logic to check the Ready condition only kicks in when
// the condition is set. Tigera takes some time to set the condition, so this
// waits until the condition is present and True.
type tigeraStatusReader struct{}

func (r *tigeraStatusReader) Supports(gk schema.GroupKind) bool {
	return gk.Group == "operator.tigera.io" // All types have the same condition structure and semantics
}

func (r *tigeraStatusReader) ReadStatus(ctx context.Context, reader engine.ClusterReader, id object.ObjMetadata) (*event.ResourceStatus, error) {
	u := &unstructured.Unstructured{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, u)
	if err != nil {
		return nil, err
	}
	return r.ReadStatusForObject(ctx, reader, u)
}

func (r *tigeraStatusReader) ReadStatusForObject(_ context.Context, _ engine.ClusterReader, u *unstructured.Unstructured) (*event.ResourceStatus, error) {
	id := object.ObjMetadata{
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
		GroupKind: u.GroupVersionKind().GroupKind(),
	}

	conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, item := range conditions {
		cond, ok := item.(map[string]any)
		if !ok {
			continue
		}

		if cond["observedGeneration"] != u.GetGeneration() {
			return &event.ResourceStatus{
				Identifier: id,
				Status:     status.InProgressStatus,
				Message:    fmt.Sprintf("needs to be reconciled %d->%d", cond["observedGeneration"], u.GetGeneration()),
			}, nil
		}

		if cond["type"] == "Ready" && cond["status"] == "True" {
			return &event.ResourceStatus{
				Identifier: id,
				Status:     status.CurrentStatus,
				Message:    "component is ready",
			}, nil
		}
	}

	return &event.ResourceStatus{
		Identifier: id,
		Status:     status.InProgressStatus,
		Message:    "waiting for Ready=True",
	}, nil
}
