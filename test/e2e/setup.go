//go:build e2e

package e2e

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tigerav1 "github.com/tigera/operator/api/v1"
	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/chart/loader"
	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capiclient "sigs.k8s.io/cluster-api/cmd/clusterctl/client"
	capiconfig "sigs.k8s.io/cluster-api/cmd/clusterctl/client/config"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"
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
	driverImage           = "dra-driver-sandbox:latest"
	kubernetesVersion     = ""
)

func init() {
	flag.StringVar(&bootstrapClusterName, "bootstrap-cluster-name", bootstrapClusterName, "name for the kind cluster used to manage Cluster API clusters")
	flag.BoolVar(&skipCleanup, "skip-cleanup", skipCleanup, "Do not delete anything that was created during the test")
	flag.StringVar(&driverManifestDirPath, "driver-manifest-dir-path", driverManifestDirPath, "path to the directory containing YAML files defining the driver")
	flag.StringVar(&testManifestDirPath, "test-manifest-dir-path", testManifestDirPath, "path to the directory containing YAML files defining test workloads")
	flag.StringVar(&driverImage, "driver-image", driverImage, "Full name of the DRA driver's container image")
	flag.StringVar(&kubernetesVersion, "kubernetes-version", kubernetesVersion, "Kubernetes version used for clusters hosting tests")
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

	kindBootstrap = kind.NewProvider(kind.ProviderWithLogger(kindLogAdapter{logger.V(5)}))
	err := kindBootstrap.Create(bootstrapClusterName, kind.CreateWithV1Alpha4Config(
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
	))
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

	kubeConfigData, err := kindBootstrap.KubeConfig(bootstrapClusterName, false)
	if err != nil {
		t.Fatal("Error getting kubeconfig for management cluster:", err)
	}
	kubeConfigFile, err := os.Create(filepath.Join(t.TempDir(), "kubeconfig"))
	if err != nil {
		t.Fatal("Error creating temp file for management cluster kubeconfig:", err)
	}
	_, err = io.Copy(kubeConfigFile, strings.NewReader(kubeConfigData))
	if err != nil {
		t.Fatal("Error writing kubeconfig:", err)
	}
	capiKubeConfig := capiclient.Kubeconfig{
		Path: kubeConfigFile.Name(),
	}
	logger.V(4).Info("kubeconfig written", "path", capiKubeConfig.Path)

	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfigFile.Name()},
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

	logger.V(5).Info("Waiting for all Nodes to be Ready")
	nodeIsReady := func(node corev1.Node) bool {
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady &&
				cond.Status == corev1.ConditionTrue {
				return true
			}
		}
		return false
	}
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		nodes := new(corev1.NodeList)
		err := client.List(ctx, nodes, &ctrlclient.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, node := range nodes.Items {
			if !nodeIsReady(node) {
				logger.V(6).Info("Node is not Ready", "node", klog.KObj(&node))
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatal("Error waiting for Nodes to become ready:", err)
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
		Kubeconfig:              capiKubeConfig,
		WaitProviders:           true,
		InfrastructureProviders: []string{capiconfig.DockerProviderName},
	})
	if err != nil {
		t.Fatal("Error initializing CAPI providers:", err)
	}

	return clusterHandle{
		kubeConfig: capiKubeConfig.Path,
		client:     client,
	}
}

func createCluster(ctx context.Context, t *testing.T, h clusterHandle, name string) clusterHandle {
	logger := klog.FromContext(ctx)

	clusterNamespace := "default"

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
	template, err := clusterctl.GetClusterTemplate(ctx, capiclient.GetClusterTemplateOptions{
		Kubeconfig:  capiclient.Kubeconfig{Path: h.kubeConfig},
		ClusterName: name,
		ProviderRepositorySource: &capiclient.ProviderRepositorySourceOptions{
			Flavor: "development",
		},
		KubernetesVersion:  kubernetesVersion,
		WorkerMachineCount: new(int64(1)),
	})
	if err != nil {
		t.Fatal("Error getting CAPI template:", err)
	}
	templateYAML, err := template.Yaml()
	if err != nil {
		t.Fatal("Error getting YAML for cluster template:", err)
	}

	clusterKustomization := &types.Kustomization{
		Patches: []types.Patch{
			{
				Patch: `{
					"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2",
					"kind": "DockerMachineTemplate",
					"metadata": {
						"name": "quick-start-default-worker-machinetemplate",
						"namespace": "` + clusterNamespace + `"
					},
					"spec": {
						"template": {
							"spec": {
								"preLoadImages": ["` + driverImage + `"]
							}
						}
					}
				}`,
			},
			{
				Patch: `{
					"apiVersion": "cluster.x-k8s.io/v1beta2",
					"kind": "Cluster",
					"metadata": {
						"name": "` + name + `",
						"namespace": "` + clusterNamespace + `"
					},
					"spec": {
						"topology": {
							"variables": [
								{
									"name": "podSecurityStandard",
									"value": {
										"enabled": false
									}
								}
							]
						}
					}
				}`,
			},
			{
				Patch: `{
					"apiVersion": "controlplane.cluster.x-k8s.io/v1beta2",
					"kind": "KubeadmControlPlaneTemplate",
					"metadata": {
						"name": "quick-start-control-plane",
						"namespace": "` + clusterNamespace + `"
					},
					"spec": {
						"template": {
							"spec": {
								"kubeadmConfigSpec": {
									"clusterConfiguration": {
										"apiServer": {
											"extraArgs": [
												{"name": "runtime-config", "value": "api/all=true"},
												{"name": "feature-gates", "value": "AllAlpha=true,AllBeta=true"}
											]
										},
										"controllerManager": {
											"extraArgs": [
												{"name": "feature-gates", "value": "AllAlpha=true,AllBeta=true"}
											]
										},
										"scheduler": {
											"extraArgs": [
												{"name": "feature-gates", "value": "AllAlpha=true,AllBeta=true"}
											]
										}
									},
									"initConfiguration": {
										"nodeRegistration": {
											"kubeletExtraArgs": [
												{"name": "feature-gates", "value": "AllAlpha=true,AllBeta=true"}
											]
										}
									},
									"joinConfiguration": {
										"nodeRegistration": {
											"kubeletExtraArgs": [
												{"name": "feature-gates", "value": "AllAlpha=true,AllBeta=true"}
											]
										}
									}
								}
							}
						}
					}
				}`,
			},
			{
				Patch: `{
					"apiVersion": "bootstrap.cluster.x-k8s.io/v1beta2",
					"kind": "KubeadmConfigTemplate",
					"metadata": {
						"name": "quick-start-default-worker-bootstraptemplate",
						"namespace": "` + clusterNamespace + `"
					},
					"spec": {
						"template": {
							"spec": {
								"joinConfiguration": {
									"nodeRegistration": {
										"kubeletExtraArgs": [
											{"name": "feature-gates", "value": "AllAlpha=true"}
										]
									}
								}
							}
						}
					}
				}`,
			},
		},
	}
	kustomizedCluster, err := kustomizeYAML(templateYAML, clusterKustomization)
	if err != nil {
		t.Fatal("Error running kustomize for cluster template:", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 10*time.Second, 1*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		for _, obj := range kustomizedCluster {
			err := h.client.Apply(ctx, ctrlclient.ApplyConfigurationFromUnstructured(&obj), ctrlclient.FieldOwner(managedFieldOwner))
			// API server returns 500 when webhooks are unreachable
			if apierrors.IsInternalError(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatal("Error applying cluster template:", err)
	}
	t.Cleanup(func() {
		cluster := &clusterv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: clusterNamespace,
			},
		}
		logger := logger.WithValues("cluster", klog.KObj(cluster))
		func() {
			cluster, err := util.GetClusterByName(ctx, h.client, clusterNamespace, name)
			if err != nil {
				t.Errorf("Error getting Cluster %s/%s: %v", clusterNamespace, name, err)
				return
			}
			clusterYAML, err := yaml.Marshal(cluster)
			if err != nil {
				t.Errorf("Error marshaling Cluster %s/%s to YAML: %v", clusterNamespace, name, err)
				return
			}
			if err := os.WriteFile(filepath.Join(t.ArtifactDir(), "cluster.yaml"), clusterYAML, 0644); err != nil {
				t.Errorf("Error writing Cluster %s/ %s: %v", clusterNamespace, name, err)
				return
			}
		}()
		if skipCleanup {
			return
		}
		err = h.client.Delete(ctx, cluster)
		if err != nil {
			logger.Error(err, "error deleting cluster")
			return
		}
		err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
			err := h.client.Get(ctx, ctrlclient.ObjectKeyFromObject(cluster), &clusterv1.Cluster{}, &ctrlclient.GetOptions{})
			if !apierrors.IsNotFound(err) {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			logger.Error(err, "error waiting for cluster to be gone", "cluster", klog.KObj(cluster))
			return
		}
	})

	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 2*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		cluster, err := util.GetClusterByName(ctx, h.client, clusterNamespace, name)
		if err != nil {
			t.Fatalf("Error getting Cluster %s: %v", name, err)
		}
		return conditions.IsTrue(cluster, clusterv1.ClusterControlPlaneInitializedCondition), nil
	})
	if err != nil {
		t.Fatal("Error waiting for ControlPlane to initialize:", err)
	}

	kubeConfigFile, err := os.Create(filepath.Join(t.TempDir(), "kubeconfig"))
	if err != nil {
		t.Fatal("Error creating workload cluster kubeconfig:", err)
	}
	kubeConfigData, err := clusterctl.GetKubeconfig(ctx, capiclient.GetKubeconfigOptions{
		Kubeconfig:          capiclient.Kubeconfig{Path: h.kubeConfig},
		WorkloadClusterName: name,
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
	sb := runtime.NewSchemeBuilder(
		scheme.AddToScheme,
		tigerav1.AddToScheme,
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

	tigeraOperator := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tigera-operator",
		},
	}
	err = client.Create(ctx, tigeraOperator)
	if err != nil {
		t.Fatalf("Error creating %s namespace: %v", tigeraOperator.Name, err)
	}

	logger.V(3).Info("Installing CNI")
	helmEnv := cli.New()
	helmConfig := action.NewConfiguration(action.ConfigurationSetLogger(helmLogger(ctx, t)))
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
	calicoCRDPath, err := calicoCharts.LocateChart("crd.projectcalico.org.v1", helmEnv)
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
	calicoInstall.WaitOptions = []kube.WaitOption{kube.WithWaitContext(ctx)}
	calicoPath, err := calicoCharts.LocateChart("tigera-operator", helmEnv)
	if err != nil {
		t.Fatal("Error locating Calico chart:", err)
	}
	calicoChart, err := loader.Load(calicoPath)
	if err != nil {
		t.Fatal("Error loading Calico chart:", err)
	}
	_, err = calicoInstall.RunWithContext(ctx, calicoChart, map[string]any{
		"goldmane": map[string]any{"enabled": false},
		"whisker":  map[string]any{"enabled": false},
	})
	if err != nil {
		t.Fatal("Error installing Calico chart:", err)
	}
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		installation := &tigerav1.Installation{}
		if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: tigeraOperator.Name, Name: "default"}, installation); err != nil {
			return false, fmt.Errorf("get Tigera Installation: %w", err)
		}
		var hasReadyCond bool
		for _, cond := range installation.Status.Conditions {
			if cond.Type != string(tigerav1.ComponentReady) {
				continue
			}
			hasReadyCond = true
			if cond.ObservedGeneration != installation.Generation {
				logger.V(5).Info("Tigera Installation needs to be reconciled")
				return false, nil
			}
			if cond.Status != metav1.ConditionStatus(tigerav1.ConditionTrue) {
				logger.V(5).Info("Tigera Installation is not ready", "status", cond.Status, "reason", cond.Reason, "message", cond.Message)
				return false, nil
			}
		}
		if !hasReadyCond {
			logger.V(5).Info("Tigera Installation is missing condition", "type", tigerav1.ComponentReady)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatal("Tigera Installation never became Ready:", err)
	}

	// wait for CAPI ControlPlane, MachineDeployments, MachinePools
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 2*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		cluster, err := util.GetClusterByName(ctx, h.client, clusterNamespace, name)
		if err != nil {
			return false, fmt.Errorf("get Cluster %s: %w", name, err)
		}
		return conditions.IsTrue(cluster, clusterv1.ClusterControlPlaneMachinesReadyCondition) &&
			conditions.IsTrue(cluster, clusterv1.ClusterWorkerMachinesReadyCondition), nil
	})
	if err != nil {
		t.Fatal("Machines never became Ready:", err)
	}

	logger.V(3).Info("Installing DRA driver")
	driverNamespace := "default"
	driverKustomization := &types.Kustomization{
		Namespace: driverNamespace,
	}
	kustomizedDriver, err := kustomizeDirectory(driverManifestDirPath, driverKustomization)
	if err != nil {
		t.Fatal("Error running kustomize for driver manifests:", err)
	}
	for _, obj := range kustomizedDriver {
		logger.V(4).Info("Creating driver resource", "apiVersion", obj.GetAPIVersion(), "kind", obj.GetKind(), "namespace", obj.GetNamespace(), "name", obj.GetName())
		err = client.Apply(ctx, ctrlclient.ApplyConfigurationFromUnstructured(&obj), ctrlclient.FieldOwner(managedFieldOwner))
		if err != nil {
			t.Fatalf("Error applying %s %s/%s: %v", obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
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

	return clusterHandle{
		kubeConfig: kubeConfigFile.Name(),
		client:     client,
	}
}
