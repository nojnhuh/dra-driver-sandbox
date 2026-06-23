//go:build e2e

package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/kustomize/api/types"
)

func TestDriver(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	c := createManagementCluster(ctx, t)

	t.Run("default", func(t *testing.T) {
		t.Parallel()
		_, ctx := ktesting.NewTestContext(t)
		c := createCluster(ctx, t, c, "default")

		tests := []struct {
			name            string
			manifest        string
			namespaceLabels map[string]string
		}{
			{
				name:     "test",
				manifest: "test.yaml",
			},
			{
				name:     "partitionable",
				manifest: "partitionable.yaml",
			},
			{
				name:     "extended-resources",
				manifest: "extended-resources.yaml",
			},
			{
				name:     "consumable-capacity",
				manifest: "consumable-capacity.yaml",
			},
			{
				name:     "admin-access",
				manifest: "admin-access.yaml",
				namespaceLabels: map[string]string{
					"resource.kubernetes.io/admin-access": "true",
				},
			},
			{
				name:     "podgroup",
				manifest: "podgroup.yaml",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				_, ctx := ktesting.NewTestContext(t)
				testManifest(ctx, t, c, test.manifest, test.namespaceLabels)
			})
		}
	})
}

func testManifest(ctx context.Context, t *testing.T, c clusterHandle, name string, namespaceLabels map[string]string) {
	logger := klog.FromContext(ctx)

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "e2e-",
			Labels:       namespaceLabels,
		},
	}
	err := c.client.Create(ctx, namespace)
	if err != nil {
		t.Fatal("Error creating namespace:", err)
	}
	t.Cleanup(func() {
		if skipCleanup {
			return
		}
		err = c.client.Delete(ctx, namespace)
		if err != nil {
			t.Errorf("Error deleting namespace %s: %v", namespace.Name, err)
			return
		}
		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true /*immediate*/, func(ctx context.Context) (bool, error) {
			err := c.client.Get(ctx, ctrlclient.ObjectKeyFromObject(namespace), namespace, &ctrlclient.GetOptions{})
			if !apierrors.IsNotFound(err) {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			t.Errorf("Error waiting for namespace %s to be gone: %v", namespace.Name, err)
			logger.Info("Namespace dump", "namespace", klog.Format(namespace))
			return
		}
	})

	k := &types.Kustomization{
		Namespace: namespace.Name,
	}
	kustomized, err := kustomizeFile(filepath.Join(testManifestDirPath, name), k)
	if err != nil {
		t.Fatal("Error running kustomize for test manifest:", err)
	}
	for _, obj := range kustomized {
		logger.V(4).Info("Creating test resource", "apiVersion", obj.GetAPIVersion(), "kind", obj.GetKind(), "namespace", obj.GetNamespace(), "name", obj.GetName())
		err = c.client.Apply(ctx, ctrlclient.ApplyConfigurationFromUnstructured(&obj), ctrlclient.FieldOwner(managedFieldOwner))
		if err != nil {
			t.Fatalf("Error applying %s %s/%s: %v", obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
		}
	}
	t.Cleanup(func() {
		if skipCleanup {
			return
		}
		for _, obj := range kustomized {
			err := c.client.Delete(ctx, &obj)
			if err != nil {
				t.Errorf("Error deleting test object %s %s %s/%s: %v", obj.GetAPIVersion(), obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
				return
			}
		}
		for _, obj := range kustomized {
			err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true /*immediate*/, func(ctx context.Context) (bool, error) {
				err := c.client.Get(ctx, ctrlclient.ObjectKeyFromObject(&obj), &obj, &ctrlclient.GetOptions{})
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return true, nil
			})
			if err != nil {
				t.Errorf("Error waiting for test object %s %s %s/%s to be gone: %v", obj.GetAPIVersion(), obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
				logger.Info("Object dump", "object", klog.Format(obj))
				return
			}
		}
	})

	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 1*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		deployments := new(appsv1.DeploymentList)
		err := c.client.List(ctx, deployments, ctrlclient.InNamespace(namespace.Name))
		if err != nil {
			return false, err
		}
		for _, deployment := range deployments.Items {
			if deployment.Status.ObservedGeneration != deployment.Generation ||
				deployment.Status.Replicas != *deployment.Spec.Replicas ||
				deployment.Status.Replicas != deployment.Status.AvailableReplicas {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Deployments never became fully available: %v", err)
	}
}
