//go:build e2e

package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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
		logger, ctx := ktesting.NewTestContext(t)
		c := createCluster(ctx, t, c, "default")

		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "e2e-",
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
				logger.Error(err, "Error deleting namespace", "namespace", namespace.Name)
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
				logger.Error(err, "error waiting for namespace to be gone", "namespace", klog.KObj(namespace))
				return
			}
		})

		k := &types.Kustomization{
			Namespace: namespace.Name,
		}
		kustomized, err := kustomizeFile(filepath.Join(testManifestDirPath, "test.yaml"), k)
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
				err = c.client.Delete(ctx, &obj)
				if err != nil {
					logger.Error(err, "Error deleting test object", "apiVersion", obj.GetAPIVersion(), "kind", obj.GetKind(), "namespace", obj.GetNamespace(), "name", obj.GetName())
					return
				}
				err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true /*immediate*/, func(ctx context.Context) (bool, error) {
					err := c.client.Get(ctx, ctrlclient.ObjectKeyFromObject(&obj), &obj, &ctrlclient.GetOptions{})
					if !apierrors.IsNotFound(err) {
						return false, err
					}
					return true, nil
				})
				if err != nil {
					logger.Error(err, "error waiting for test object to be gone", "apiVersion", obj.GetAPIVersion(), "kind", obj.GetKind(), "namespace", obj.GetNamespace(), "name", obj.GetName())
					return
				}
			}
		})

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod",
				Namespace: namespace.Name,
			},
		}
		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 1*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
			err := c.client.Get(ctx, ctrlclient.ObjectKeyFromObject(pod), pod)
			if err != nil {
				return false, err
			}
			return pod.Status.Phase == corev1.PodSucceeded, nil
		})
		if err != nil {
			t.Fatalf("Pod %s/%s never succeeded: %v", pod.Namespace, pod.Name, err)
		}
	})
}
