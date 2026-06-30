//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
	capiyaml "sigs.k8s.io/cluster-api/util/yaml"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/kustomize/api/types"
)

func TestDriver(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	c := createManagementCluster(ctx, t)

	type manifestTest struct {
		name            string
		manifest        string
		namespaceLabels map[string]string
	}

	defaultTests := []manifestTest{
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
	}
	alphaTests := []manifestTest{
		{
			name:     "podgroup",
			manifest: "podgroup.yaml",
		},
	}

	t.Run("default-ga", func(t *testing.T) {
		t.Parallel()
		_, ctx := ktesting.NewTestContext(t)

		cluster := buildDefaultCluster(defaultClusterOpts{
			runtimeConfig: "admissionregistration.k8s.io/v1beta1=true", // For calico chart
		})
		c := createCluster(ctx, t, c, cluster)

		for _, test := range defaultTests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				_, ctx := ktesting.NewTestContext(t)
				testManifest(ctx, t, c, test.manifest, test.namespaceLabels)
			})
		}
	})

	t.Run("default-alpha", func(t *testing.T) {
		t.Parallel()
		_, ctx := ktesting.NewTestContext(t)

		cluster := buildDefaultCluster(defaultClusterOpts{
			namePrefix:    "alpha",
			featureGates:  "AllBeta=true,AllAlpha=true",
			runtimeConfig: "api/all=true",
		})
		c := createCluster(ctx, t, c, cluster)

		for _, test := range append(defaultTests, alphaTests...) {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				_, ctx := ktesting.NewTestContext(t)
				testManifest(ctx, t, c, test.manifest, test.namespaceLabels)
			})
		}
	})

	t.Run("azure-ga", func(t *testing.T) {
		t.Parallel()
		_, ctx := ktesting.NewTestContext(t)

		cluster := buildAzureCluster(azureClusterOpts{
			runtimeConfig: "admissionregistration.k8s.io/v1beta1=true", // For calico chart
		})
		c := createCluster(ctx, t, c, cluster)

		for _, test := range defaultTests {
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
	if err := os.WriteFile(filepath.Join(t.ArtifactDir(), "manifest.yaml"), kustomized, 0644); err != nil {
		t.Error("Error writing manifest artifact:", err)
	}
	objs, err := capiyaml.ToUnstructured(kustomized)
	if err != nil {
		t.Fatal("Error converting manifest objects to unstructured:", err)
	}
	for _, obj := range objs {
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
		for _, obj := range objs {
			err := c.client.Delete(ctx, &obj)
			if err != nil {
				t.Errorf("Error deleting test object %s %s %s/%s: %v", obj.GetAPIVersion(), obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
				return
			}
		}
		for _, obj := range objs {
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
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		lists := []ctrlclient.ObjectList{
			&appsv1.DeploymentList{},
			&corev1.PodList{},
			&resourcev1.ResourceClaimList{},
			&corev1.EventList{},
		}
		for _, list := range lists {
			err := c.client.List(ctx, list, ctrlclient.InNamespace(namespace.Name))
			if err != nil {
				t.Errorf("Error getting %T: %v", list, err)
				continue
			}
			err = meta.EachListItem(list, func(o runtime.Object) error {
				logger.V(5).Info("Object dump", "type", fmt.Sprintf("%T", o), "obj", klog.Format(o))
				return nil
			})
			if err != nil {
				t.Errorf("Error iterating over %T: %v", list, err)
				continue
			}
		}
	})

	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 2*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
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
		t.Errorf("Deployments never became fully available: %v", err)
	} else {
		logger.Info("All Deployment Pods scheduled successfully")
	}
}
