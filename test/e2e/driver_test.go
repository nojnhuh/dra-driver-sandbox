//go:build e2e

package e2e

import (
	"testing"

	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
)

func TestDriver(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	h := createManagementCluster(ctx, t)

	t.Run("default", func(t *testing.T) {
		t.Parallel()
		_, ctx := ktesting.NewTestContext(t)
		createCluster(ctx, t, h, "default")
	})
}
