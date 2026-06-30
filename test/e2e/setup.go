//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"
	"github.com/go-logr/logr"
	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/chart/loader"
	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capiclient "sigs.k8s.io/cluster-api/cmd/clusterctl/client"
	capiconfig "sigs.k8s.io/cluster-api/cmd/clusterctl/client/config"
	"sigs.k8s.io/cluster-api/util/conditions"
	capiyaml "sigs.k8s.io/cluster-api/util/yaml"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	kindv1 "sigs.k8s.io/kind/pkg/apis/config/v1alpha4"
	kind "sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"
)

var (
	bootstrapClusterName  = "dra-driver-sandbox"
	skipCleanup           = false
	driverManifestDirPath = filepath.Join("..", "..", "deploy")
	testManifestDirPath   = filepath.Join("..", "..", "manifests")
	clusterDirPath        = filepath.Join("..", "..", "clusters")
	kubernetesVersion     = ""

	// TODO: define and use ClusterClass variables for these
	azureTenantID                                  = os.Getenv("AZURE_TENANT_ID")
	azureSubscriptionID                            = os.Getenv("AZURE_SUBSCRIPTION_ID")
	azureClientID                                  = os.Getenv("AZURE_CLIENT_ID")
	azureClusterIdentityType                       = os.Getenv("CLUSTER_IDENTITY_TYPE")
	azureLocation                                  = os.Getenv("AZURE_LOCATION")
	azureControlPlaneMachineType                   = os.Getenv("AZURE_CONTROL_PLANE_MACHINE_TYPE")
	azureNodeMachineType                           = os.Getenv("AZURE_NODE_MACHINE_TYPE")
	azureSSHPublicKey                              = os.Getenv("AZURE_SSH_PUBLIC_KEY")
	azureMachineUserAssignedIdentitySubscriptionID = os.Getenv("AZURE_SUBSCRIPTION_ID")
	azureMachineUserAssignedIdentityResourceGroup  = os.Getenv("CI_RG")
	azureMachineUserAssignedIdentityName           = os.Getenv("USER_IDENTITY")

	kubeletPluginImage = parsedImageRef{
		Name:    "dra-driver-sandbox-kubeletplugin",
		NewName: "dra-driver-sandbox-kubeletplugin", // don't change by default
	}
	controllerImage = parsedImageRef{
		Name:    "dra-driver-sandbox-controller",
		NewName: "dra-driver-sandbox-controller", // don't change by default
	}
)

func init() {
	flag.StringVar(&bootstrapClusterName, "bootstrap-cluster-name", bootstrapClusterName, "name for the kind cluster used to manage Cluster API clusters")
	flag.BoolVar(&skipCleanup, "skip-cleanup", skipCleanup, "Do not delete anything that was created during the test")
	flag.StringVar(&driverManifestDirPath, "driver-manifest-dir-path", driverManifestDirPath, "path to the directory containing YAML files defining the driver")
	flag.StringVar(&testManifestDirPath, "test-manifest-dir-path", testManifestDirPath, "path to the directory containing YAML files defining test workloads")
	flag.Var(&kubeletPluginImage, "kubelet-plugin-image", "Full name of the DRA driver's kubelet plugin container image")
	flag.Var(&controllerImage, "controller-image", "Full name of the DRA driver's controller container image")
	flag.StringVar(&kubernetesVersion, "kubernetes-version", kubernetesVersion, "Kubernetes version used for clusters hosting tests")

	flag.StringVar(&azureTenantID, "azure-tenant-id", azureTenantID, "Azure tenant ID")
	flag.StringVar(&azureSubscriptionID, "azure-subscription-id", azureSubscriptionID, "Azure subscription ID")
	flag.StringVar(&azureClientID, "azure-client-id", azureClientID, "Azure client ID")
	flag.StringVar(&azureClusterIdentityType, "azure-cluster-identity-type", azureClusterIdentityType, "AzureClusterIdentity spec.type")
	flag.StringVar(&azureLocation, "azure-location", azureLocation, "Azure location")
	flag.StringVar(&azureControlPlaneMachineType, "azure-control-plane-machine-type", azureControlPlaneMachineType, "Azure control plane machine type")
	flag.StringVar(&azureNodeMachineType, "azure-node-machine-type", azureNodeMachineType, "Azure node machine type")
	flag.StringVar(&azureSSHPublicKey, "azure-ssh-public-key", azureSSHPublicKey, "Azure SSH public key")
	flag.StringVar(&azureMachineUserAssignedIdentitySubscriptionID, "azure-machine-user-assigned-identity-subscription-id", azureMachineUserAssignedIdentitySubscriptionID, "Azure subscription ID of the user-assigned managed identity added to control plane and worker machines")
	flag.StringVar(&azureMachineUserAssignedIdentityResourceGroup, "azure-machine-user-assigned-identity-resource-group", azureMachineUserAssignedIdentityResourceGroup, "Azure resource group name of the user-assigned managed identity added to control plane and worker machines")
	flag.StringVar(&azureMachineUserAssignedIdentityName, "azure-machine-user-assigned-identity-name", azureMachineUserAssignedIdentityName, "Azure name of the user-assigned managed identity added to control plane and worker machines")
}

type parsedImageRef types.Image

// Set implements [flag.Value].
func (r *parsedImageRef) Set(val string) error {
	parsed, err := reference.Parse(val)
	if err != nil {
		return err
	}
	if named, ok := parsed.(reference.Named); ok {
		r.NewName = named.Name()
	}
	if tagged, ok := parsed.(reference.Tagged); ok {
		r.NewTag = tagged.Tag()
	}
	if digested, ok := parsed.(reference.Digested); ok {
		r.Digest = digested.Digest().String()
	}
	return nil
}

// String implements [flag.Value].
func (r *parsedImageRef) String() string {
	var s strings.Builder
	s.WriteString(r.NewName)
	if r.NewTag != "" {
		s.WriteRune(':')
		s.WriteString(r.NewTag)
	}
	if r.Digest != "" {
		s.WriteRune('@')
		s.WriteString(r.Digest)
	}
	return s.String()
}

var (
	kindBootstrap     *kind.Provider
	managedFieldOwner = "dra-driver-sandbox-e2e"
)

type clusterHandle struct {
	// Path to the kubeconfig
	kubeConfig string
	client     ctrlclient.Client
}

func createManagementCluster(ctx context.Context, t *testing.T) clusterHandle {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("creating management cluster")

	kindBootstrap = kind.NewProvider(kind.ProviderWithLogger(kindLogAdapter{logger.V(5).WithName("kind")}))
	kubeConfigPath := filepath.Join(t.TempDir(), "kubeconfig")
	err := kindBootstrap.Create(bootstrapClusterName,
		kind.CreateWithKubeconfigPath(kubeConfigPath),
		kind.CreateWithWaitForReady(2*time.Minute),
		kind.CreateWithV1Alpha4Config(
			&kindv1.Cluster{
				Nodes: []kindv1.Node{
					{
						Role: kindv1.ControlPlaneRole,
						ExtraMounts: []kindv1.Mount{
							{
								HostPath:      "/var/run/docker.sock",
								ContainerPath: "/var/run/docker.sock",
							},
						},
					},
				},
			},
		),
	)
	if err != nil {
		t.Fatal("Error creating management cluster:", err)
	}
	t.Cleanup(func() {
		if skipCleanup {
			return
		}
		logger.V(4).Info("deleting management cluster")
		err := kindBootstrap.Delete(bootstrapClusterName, "")
		if err != nil {
			t.Errorf("Error deleting bootstrap cluster %s: %v", bootstrapClusterName, err)
			return
		}
	})
	t.Cleanup(func() {
		logger.V(4).Info("collecting kind logs", "name", bootstrapClusterName, "dir", t.ArtifactDir())
		err := kindBootstrap.CollectLogs(bootstrapClusterName, t.ArtifactDir())
		if err != nil {
			t.Errorf("Error collecting kind logs for cluster %s: %v", bootstrapClusterName, err)
			return
		}
	})
	logger.V(4).Info("created management cluster", "kubeconfig", kubeConfigPath)

	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfigPath},
		&clientcmd.ConfigOverrides{},
	)
	clientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		t.Fatal("Error getting client config:", err)
	}
	sb := runtime.NewSchemeBuilder(
		scheme.AddToScheme,
		clusterv1.AddToScheme,
	)
	s := runtime.NewScheme()
	if err := sb.AddToScheme(s); err != nil {
		t.Fatal("Error building scheme:", err)
	}
	client, err := ctrlclient.New(clientConfig, ctrlclient.Options{
		Scheme: s,
	})
	if err != nil {
		t.Fatal("Error building Kubernetes client:", err)
	}

	logger.V(5).Info("Initializing Cluster API providers")
	capiConfigReader := capiconfig.NewMemoryReader()
	if err := capiConfigReader.Init(ctx, ""); err != nil {
		t.Fatal("Error initializing CAPI memory config reader:", err)
	}
	capiConfigReader.Set("CLUSTER_TOPOLOGY", "true")
	capiConfig, err := capiconfig.New(ctx, "", capiconfig.InjectReader(capiConfigReader))
	if err != nil {
		t.Fatal("Error initializing CAPI config:", err)
	}
	clusterctl, err := capiclient.New(ctx, "", capiclient.InjectConfig(capiConfig))
	if err != nil {
		t.Fatal("Error creating CAPI client:", err)
	}
	_, err = clusterctl.Init(ctx, capiclient.InitOptions{
		Kubeconfig: capiclient.Kubeconfig{
			Path: kubeConfigPath,
		},
		WaitProviders: true,
		InfrastructureProviders: []string{
			capiconfig.DockerProviderName,
			capiconfig.AzureProviderName,
		},
		AddonProviders: []string{
			capiconfig.HelmAddonProviderName,
		},
	})
	if err != nil {
		t.Fatal("Error initializing CAPI providers:", err)
	}

	clusterClassesPath := filepath.Join(clusterDirPath, "capi")
	clusterClassesDir, err := os.ReadDir(clusterClassesPath)
	if err != nil {
		t.Fatal("Error reading ClusterClass directory:", err)
	}
	clusterClassNames := sets.New[ctrlclient.ObjectKey]()
	for _, entry := range clusterClassesDir {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		logger.V(5).Info("Creating ClusterClass", "filename", entry.Name())
		template, err := os.ReadFile(filepath.Join(clusterClassesPath, entry.Name()))
		if err != nil {
			t.Fatal("Error reading cluster template:", err)
		}
		unstructuredObjs, err := capiyaml.ToUnstructured(template)
		if err != nil {
			t.Fatal("Error converting cluster template to unstructured:", err)
		}
		err = wait.PollUntilContextTimeout(ctx, 10*time.Second, 1*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
			for _, obj := range unstructuredObjs {
				err := client.Apply(ctx, ctrlclient.ApplyConfigurationFromUnstructured(&obj), ctrlclient.FieldOwner(managedFieldOwner))
				// API server returns 500 when webhooks are unreachable
				// and when patches are invalid(???)
				if apierrors.IsInternalError(err) && strings.Contains(err.Error(), "failed calling webhook") {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				apiGroup, _, _ := strings.Cut(obj.GetAPIVersion(), "/")
				if apiGroup == clusterv1.GroupVersion.Group && obj.GetKind() == clusterv1.ClusterClassKind {
					clusterClassNames.Insert(ctrlclient.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()})
				}
			}
			return true, nil
		})
		if err != nil {
			t.Fatal("Error applying ClusterClass template:", err)
		}
	}

	t.Cleanup(func() {
		var clusterClasses []*clusterv1.ClusterClass
		func() {
			for clusterClassKey := range clusterClassNames {
				clusterClass := &clusterv1.ClusterClass{}
				err := client.Get(ctx, clusterClassKey, clusterClass)
				if err != nil {
					t.Errorf("Error getting ClusterClass %s: %v", clusterClassKey, err)
					return
				}
				clusterClasses = append(clusterClasses, clusterClass)

				clusterClassYAML, err := yaml.Marshal(clusterClass)
				if err != nil {
					t.Errorf("Error marshaling ClusterClass to YAML: %v", err)
					return
				}
				clusterClassPath := filepath.Join(t.ArtifactDir(), "clusterclass", clusterClassKey.Namespace)
				if err := os.MkdirAll(clusterClassPath, 0755); err != nil {
					t.Errorf("Error creating directory for ClusterClass %s: %v", clusterClassKey, err)
					return
				}
				if err := os.WriteFile(filepath.Join(clusterClassPath, clusterClassKey.Name+".yaml"), clusterClassYAML, 0644); err != nil {
					t.Errorf("Error writing ClusterClass %s: %v", clusterClassKey, err)
					return
				}
			}
		}()
		if skipCleanup {
			return
		}
		for _, clusterClass := range clusterClasses {
			clusterClassKey := ctrlclient.ObjectKeyFromObject(clusterClass)
			err := client.Delete(ctx, clusterClass)
			if err != nil {
				t.Errorf("Error deleting ClusterClass %s: %v", clusterClassKey, err)
				continue
			}
			defer func() {
				err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
					err := client.Get(ctx, clusterClassKey, clusterClass)
					if !apierrors.IsNotFound(err) {
						return false, err
					}
					return true, nil
				})
				if err != nil {
					t.Errorf("Error waiting for ClusterClass %s to be gone: %v", clusterClassKey, err)
				}
			}()
		}
	})

	return clusterHandle{
		kubeConfig: kubeConfigPath,
		client:     client,
	}
}

func createCluster(ctx context.Context, t *testing.T, h clusterHandle, cluster *clusterv1.Cluster) clusterHandle {
	logger := klog.FromContext(ctx)

	clusterYAML, err := yaml.Marshal(cluster)
	if err != nil {
		t.Error("Error marshaling Cluster YAML:", err)
	} else {
		if err := os.WriteFile(filepath.Join(t.ArtifactDir(), "cluster-before-create.yaml"), clusterYAML, 0644); err != nil {
			t.Error("Error writing cluster template artifact:", err)
		}
	}

	if loggerV := logger.V(6); loggerV.Enabled() {
		logger.Info("Creating Cluster", "dump", klog.Format(cluster))
	} else {
		logger.Info("Creating Cluster")
	}

	err = h.client.Create(ctx, cluster, ctrlclient.FieldOwner(managedFieldOwner))
	if err != nil {
		t.Fatal("Error creating cluster:", err)
	}
	logger = logger.WithValues("cluster", klog.KObj(cluster))
	clusterKey := ctrlclient.ObjectKeyFromObject(cluster)
	t.Cleanup(func() {
		func() {
			err := h.client.Get(ctx, clusterKey, cluster)
			if err != nil {
				t.Errorf("Error getting Cluster %s: %v", clusterKey, err)
				return
			}
			clusterYAML, err := yaml.Marshal(cluster)
			if err != nil {
				t.Errorf("Error marshaling Cluster %s to YAML: %v", clusterKey, err)
				return
			}
			if err := os.WriteFile(filepath.Join(t.ArtifactDir(), "cluster-after-test.yaml"), clusterYAML, 0644); err != nil {
				t.Errorf("Error writing Cluster %s: %v", clusterKey, err)
				return
			}
		}()
		if skipCleanup {
			return
		}
		logger.Info("Deleting Cluster")
		err = h.client.Delete(ctx, cluster)
		if err != nil {
			t.Errorf("Error deleting Cluster %s: %v", clusterKey, err)
			return
		}
		err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 15*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
			err := h.client.Get(ctx, ctrlclient.ObjectKeyFromObject(cluster), cluster)
			if !apierrors.IsNotFound(err) {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			t.Errorf("Error waiting for Cluster %s to be gone: %v", clusterKey, err)
			return
		}
	})

	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 15*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		err := h.client.Get(ctx, clusterKey, cluster)
		if err != nil {
			t.Fatalf("Error getting Cluster %s: %v", clusterKey, err)
		}
		return conditions.IsTrue(cluster, clusterv1.ClusterControlPlaneInitializedCondition), nil
	})
	if err != nil {
		t.Fatal("Error waiting for ControlPlane to initialize:", err)
	}

	// Create a new in-memory reader independent from other tests running in
	// parallel.
	capiConfigReader := capiconfig.NewMemoryReader()
	if err := capiConfigReader.Init(ctx, ""); err != nil {
		t.Fatal("Error initializing CAPI memory config reader:", err)
	}
	capiConfig, err := capiconfig.New(ctx, "", capiconfig.InjectReader(capiConfigReader))
	if err != nil {
		t.Fatal("Error initializing CAPI config:", err)
	}
	clusterctl, err := capiclient.New(ctx, "", capiclient.InjectConfig(capiConfig))
	if err != nil {
		t.Fatal("Error creating CAPI client:", err)
	}
	kubeConfigFile, err := os.Create(filepath.Join(t.TempDir(), "kubeconfig"))
	if err != nil {
		t.Fatal("Error creating workload cluster kubeconfig:", err)
	}
	kubeConfigData, err := clusterctl.GetKubeconfig(ctx, capiclient.GetKubeconfigOptions{
		Kubeconfig:          capiclient.Kubeconfig{Path: h.kubeConfig},
		WorkloadClusterName: cluster.Name,
	})
	if err != nil {
		t.Fatal("Error getting workload cluster kubeconfig:", err)
	}
	if _, err := io.Copy(kubeConfigFile, strings.NewReader(kubeConfigData)); err != nil {
		t.Fatal("Error writing workload cluster kubeconfig:", err)
	}

	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfigFile.Name()},
		&clientcmd.ConfigOverrides{},
	)
	clientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		t.Fatal("Error getting client config:", err)
	}
	clientConfig.QPS = 20
	clientConfig.Burst = 50
	sb := runtime.NewSchemeBuilder(
		scheme.AddToScheme,
	)
	s := runtime.NewScheme()
	if err := sb.AddToScheme(s); err != nil {
		t.Fatal("Error building scheme:", err)
	}
	client, err := ctrlclient.New(clientConfig, ctrlclient.Options{
		Scheme: s,
	})
	if err != nil {
		t.Fatal("Error building controller-runtime client:", err)
	}
	k8sClient, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		t.Fatal("Error building Kubernetes client:", err)
	}

	tigeraOperator := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tigera-operator",
		},
	}
	err = client.Create(ctx, tigeraOperator)
	if err != nil {
		t.Fatalf("Error creating %s namespace: %v", tigeraOperator.Name, err)
	}

	// CAAPH would be the most CAPI-native way to do this. The Calico chart
	// relies on CRDs though and CAAPH doesn't converge very quickly if the CRDs
	// don't get installed before the resources. CAAPH doesn't make it practical
	// to synchronize those.
	logger.V(3).Info("Installing CNI")
	helmEnv := cli.New()
	helmConfig := action.NewConfiguration(action.ConfigurationSetLogger(logr.ToSlogHandler(logger.V(2).WithName("helm"))))
	helmConfigFlags := genericclioptions.NewConfigFlags(false)
	helmConfigFlags.KubeConfig = new(kubeConfigFile.Name())
	err = helmConfig.Init(helmConfigFlags, tigeraOperator.Name, "")
	if err != nil {
		t.Fatal("Error initializing Helm config:", err)
	}

	calicoCharts := action.ChartPathOptions{
		RepoURL: "https://docs.tigera.io/calico/charts",
	}

	calicoCRDInstall := action.NewInstall(helmConfig)
	calicoCRDInstall.Namespace = tigeraOperator.Name
	calicoCRDInstall.ReleaseName = "calico-crds"
	calicoCRDInstall.WaitStrategy = kube.StatusWatcherStrategy
	calicoCRDInstall.WaitOptions = []kube.WaitOption{kube.WithWaitContext(ctx)}
	calicoCRDPath, err := calicoCharts.LocateChart("projectcalico.org.v3", helmEnv)
	if err != nil {
		t.Fatal("Error locating Calico CRD chart:", err)
	}
	calicoCRDChart, err := loader.Load(calicoCRDPath)
	if err != nil {
		t.Fatal("Error loading Calico CRD chart:", err)
	}
	_, err = calicoCRDInstall.RunWithContext(ctx, calicoCRDChart, nil)
	if err != nil {
		t.Fatal("Error installing Calico CRD chart:", err)
	}

	calicoInstall := action.NewInstall(helmConfig)
	calicoInstall.ReleaseName = "calico"
	calicoInstall.Namespace = tigeraOperator.Name
	calicoInstall.WaitStrategy = kube.StatusWatcherStrategy
	calicoInstall.WaitOptions = []kube.WaitOption{
		kube.WithWaitContext(ctx),
		kube.WithKStatusReaders(&tigeraStatusReader{}),
	}
	calicoInstall.Timeout = 5 * time.Minute
	calicoPath, err := calicoCharts.LocateChart("tigera-operator", helmEnv)
	if err != nil {
		t.Fatal("Error locating Calico chart:", err)
	}
	calicoChart, err := loader.Load(calicoPath)
	if err != nil {
		t.Fatal("Error loading Calico chart:", err)
	}
	_, err = calicoInstall.RunWithContext(ctx, calicoChart, map[string]any{
		"apiServer": map[string]any{"enabled": false},
		"goldmane":  map[string]any{"enabled": false},
		"whisker":   map[string]any{"enabled": false},
	})
	if err != nil {
		t.Fatal("Error installing Calico chart:", err)
	}
	collectPods(ctx, t, k8sClient, tigeraOperator.Name, metav1.ListOptions{})
	collectPods(ctx, t, k8sClient, "calico-system", metav1.ListOptions{})

	// wait for CAPI ControlPlane, MachineDeployments, MachinePools
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 2*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		err := h.client.Get(ctx, clusterKey, cluster)
		if err != nil {
			return false, fmt.Errorf("get Cluster: %w", err)
		}
		return conditions.IsTrue(cluster, clusterv1.ClusterControlPlaneMachinesReadyCondition) &&
			conditions.IsTrue(cluster, clusterv1.ClusterWorkerMachinesReadyCondition), nil
	})
	if err != nil {
		t.Fatal("Machines never became Ready:", err)
	}
	collectPods(ctx, t, k8sClient, "kube-system", metav1.ListOptions{})

	logger.V(3).Info("Installing DRA driver")
	driverNamespace := "default"
	driverKustomization := &types.Kustomization{
		Namespace: driverNamespace,
		Images: []types.Image{
			types.Image(kubeletPluginImage),
			types.Image(controllerImage),
		},
	}
	kustomizedDriver, err := kustomizeDirectory(driverManifestDirPath, driverKustomization)
	if err != nil {
		t.Fatal("Error running kustomize for driver manifests:", err)
	}
	if err := os.WriteFile(filepath.Join(t.ArtifactDir(), "dra-driver-sandbox.yaml"), kustomizedDriver, 0644); err != nil {
		t.Error("Error writing driver components artifact:", err)
	}
	driverObjs, err := capiyaml.ToUnstructured(kustomizedDriver)
	if err != nil {
		t.Fatal("Error converting driver objects to unstructured:", err)
	}
	for _, obj := range driverObjs {
		logger.V(4).Info("Creating driver resource", "apiVersion", obj.GetAPIVersion(), "kind", obj.GetKind(), "namespace", obj.GetNamespace(), "name", obj.GetName())
		err = client.Create(ctx, &obj, ctrlclient.FieldOwner(managedFieldOwner))
		if err != nil {
			t.Fatalf("Error creating %s %s/%s: %v", obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
		}
	}
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 60*time.Second, true /*immediate*/, func(ctx context.Context) (bool, error) {
		daemonSets := new(appsv1.DaemonSetList)
		err := client.List(
			ctx,
			daemonSets,
			ctrlclient.InNamespace(driverNamespace),
			ctrlclient.HasLabels{
				"app.kubernetes.io/name=dra-driver-sandbox",
				"app.kubernetes.io/component=kubeletplugin",
			},
		)
		if err != nil {
			return false, err
		}
		for _, ds := range daemonSets.Items {
			if ds.Generation != ds.Status.ObservedGeneration ||
				ds.Status.CurrentNumberScheduled < ds.Status.DesiredNumberScheduled ||
				ds.Status.NumberReady < ds.Status.DesiredNumberScheduled {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatal("DaemonSet Pods never became Ready:", err)
	}
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 1*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		deployments := new(appsv1.DeploymentList)
		err := client.List(
			ctx,
			deployments,
			ctrlclient.InNamespace(driverNamespace),
			ctrlclient.HasLabels{
				"app.kubernetes.io/name=dra-driver-sandbox",
				"app.kubernetes.io/component=controller",
			},
		)
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
	collectPods(ctx, t, k8sClient, driverNamespace, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=dra-driver-sandbox"})

	return clusterHandle{
		kubeConfig: kubeConfigFile.Name(),
		client:     client,
	}
}

func jsonMustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

type defaultClusterOpts struct {
	namePrefix    string
	featureGates  string
	runtimeConfig string
}

func buildDefaultCluster(opts defaultClusterOpts) *clusterv1.Cluster {
	namePrefix := "default"
	if opts.namePrefix != "" {
		namePrefix = opts.namePrefix
	}
	return &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namePrefix + "-",
			Namespace:    "default",
		},
		Spec: clusterv1.ClusterSpec{
			ClusterNetwork: clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{
					CIDRBlocks: []string{"192.168.0.0/16"},
				},
				ServiceDomain: "cluster.local",
				Services: clusterv1.NetworkRanges{
					CIDRBlocks: []string{"10.128.0.0/12"},
				},
			},
			Topology: clusterv1.Topology{
				ClassRef: clusterv1.ClusterClassRef{
					Name:      "default",
					Namespace: "default",
				},
				ControlPlane: clusterv1.ControlPlaneTopology{
					Replicas: new(int32(1)),
				},
				Variables: []clusterv1.ClusterVariable{
					{
						Name: "featureGates",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(opts.featureGates),
						},
					},
					{
						Name: "runtimeConfig",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(opts.runtimeConfig),
						},
					},
					{
						Name: "preLoadImages",
						Value: apiextensionsv1.JSON{
							Raw: []byte(`[
								"` + kubeletPluginImage.String() + `",
								"` + controllerImage.String() + `"
							]`),
						},
					},
				},
				Version: kubernetesVersion,
				Workers: clusterv1.WorkersTopology{
					MachineDeployments: []clusterv1.MachineDeploymentTopology{
						{
							Class:    "worker",
							Name:     "md-0",
							Replicas: new(int32(1)),
						},
					},
				},
			},
		},
	}
}

type azureClusterOpts struct {
	namePrefix    string
	featureGates  string
	runtimeConfig string
}

func buildAzureCluster(opts azureClusterOpts) *clusterv1.Cluster {
	namePrefix := "azure"
	if opts.namePrefix != "" {
		namePrefix = opts.namePrefix
	}
	return &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namePrefix + "-",
			Namespace:    "default",
			Labels: map[string]string{
				"cloud-provider": "azure",
			},
		},
		Spec: clusterv1.ClusterSpec{
			ClusterNetwork: clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{
					CIDRBlocks: []string{"192.168.0.0/16"},
				},
			},
			Topology: clusterv1.Topology{
				ClassRef: clusterv1.ClusterClassRef{
					Name:      "azure",
					Namespace: "default",
				},
				ControlPlane: clusterv1.ControlPlaneTopology{
					Replicas: new(int32(1)),
				},
				Variables: []clusterv1.ClusterVariable{
					{
						Name: "subscriptionID",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(azureSubscriptionID),
						},
					},
					{
						Name: "location",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(azureLocation),
						},
					},
					{
						Name: "controlPlaneVMSize",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(azureControlPlaneMachineType),
						},
					},
					{
						Name: "workerVMSize",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(azureNodeMachineType),
						},
					},
					{
						Name: "sshPublicKey",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal([]byte(azureSSHPublicKey)),
						},
					},
					{
						Name: "machineUserAssignedIdentity",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(map[string]any{
								"subscriptionID": azureMachineUserAssignedIdentitySubscriptionID,
								"resourceGroup":  azureMachineUserAssignedIdentityResourceGroup,
								"name":           azureMachineUserAssignedIdentityName,
							}),
						},
					},
					{
						Name: "featureGates",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(opts.featureGates),
						},
					},
					{
						Name: "runtimeConfig",
						Value: apiextensionsv1.JSON{
							Raw: jsonMustMarshal(opts.runtimeConfig),
						},
					},
				},
				Version: kubernetesVersion,
				Workers: clusterv1.WorkersTopology{
					MachineDeployments: []clusterv1.MachineDeploymentTopology{
						{
							Class:    "worker",
							Name:     "md-0",
							Replicas: new(int32(1)),
						},
					},
				},
			},
		},
	}
}

func collectPods(ctx context.Context, t *testing.T, client kubernetes.Interface, namespace string, listOpts metav1.ListOptions) {
	logger := klog.FromContext(ctx)

	var loggingPods *corev1.PodList
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 1*time.Minute, true /*immediate*/, func(ctx context.Context) (done bool, err error) {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, listOpts)
		if err != nil {
			return false, fmt.Errorf("list pods: %w", err)
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning &&
				pod.Status.Phase != corev1.PodSucceeded &&
				pod.Status.Phase != corev1.PodFailed {
				return false, nil
			}
		}
		loggingPods = pods
		return true, nil
	})
	if err != nil {
		t.Error("Not all Pods started logging:", err)
		return
	}

	podArtifactsDir := func(pod corev1.Pod) string {
		return filepath.Join(t.ArtifactDir(), "pods", namespace, pod.Name)
	}

	var wg sync.WaitGroup
	var logStreams []io.ReadCloser
	for _, pod := range loggingPods.Items {
		podDir := podArtifactsDir(pod)
		if err := os.MkdirAll(podDir, 0755); err != nil {
			t.Errorf("Error creating directory for Pod %s/%s logs: %v", namespace, pod.Name, err)
			continue
		}
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			logReq := client.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
				Container: container.Name,
				Follow:    true,
			})
			logFile, err := os.Create(filepath.Join(podDir, container.Name+".log"))
			if err != nil {
				t.Errorf("Error creating log file for Pod %s/%s container %s: %v", namespace, pod.Name, container.Name, err)
				continue
			}
			logger := logger.WithValues("pod", klog.KObj(&pod), "container", container.Name, "path", logFile.Name())
			logger.V(5).Info("Starting log stream")
			logs, err := logReq.Stream(ctx)
			if err != nil {
				t.Errorf("Error streaming logs for Pod %s/%s container %s: %v", namespace, pod.Name, container.Name, err)
				continue
			}
			logStreams = append(logStreams, logs)
			wg.Go(func() {
				_, err := io.Copy(logFile, logs)
				if err != nil && !strings.Contains(err.Error(), "response body closed") { // "closed" is the normal case
					t.Errorf("Error writing log file for Pod %s/%s container %s: %v", namespace, pod.Name, container.Name, err)
				}
			})
		}
	}

	t.Cleanup(func() {
		for _, logs := range logStreams {
			err := logs.Close()
			if err != nil {
				logger.Error(err, "Error closing log stream")
			}
		}
		wg.Wait()
		logger.V(6).Info("Closed log streams")
	})

	t.Cleanup(func() {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, listOpts)
		if err != nil {
			t.Error("Error listing Pods:", err)
			return
		}
		for _, pod := range pods.Items {
			podDir := podArtifactsDir(pod)
			podYAML, err := yaml.Marshal(pod)
			if err != nil {
				t.Error("Error marshaling Pod to YAML:", err)
				return
			}
			if err := os.WriteFile(filepath.Join(podDir, "pod.yaml"), podYAML, 0644); err != nil {
				t.Error("Error writing Pod YAML artifact:", err)
				return
			}
		}
	})
}
