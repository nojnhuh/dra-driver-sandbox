//go:build e2e

package e2e

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/chart/loader"
	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	kindv1 "sigs.k8s.io/kind/pkg/apis/config/v1alpha4"
	kind "sigs.k8s.io/kind/pkg/cluster"
)

var (
	bootstrapClusterName = "dra-driver-template"
)

func init() {
	flag.StringVar(&bootstrapClusterName, "bootstrap-cluster-name", bootstrapClusterName, "name for the kind cluster used to manage Cluster API clusters")
}

var kindBootstrap *kind.Provider

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
		logger.V(4).Info("deleting management cluster")
		err := kindBootstrap.Delete(bootstrapClusterName, "")
		if err != nil {
			logger.Error(err, "error deleting bootstrap cluster", "name", bootstrapClusterName)
			return
		}
	})
	t.Cleanup(func() {
		logger.V(4).Info("collecting kind logs", "name", bootstrapClusterName, "dir", t.ArtifactDir())
		err := kindBootstrap.CollectLogs(bootstrapClusterName, t.ArtifactDir())
		if err != nil {
			logger.Error(err, "error collecting kind logs", "name", bootstrapClusterName)
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
		KubernetesVersion: "v1.35.1",
	})
	if err != nil {
		t.Fatal("Error getting CAPI template:", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 10*time.Second, 1*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		for _, obj := range template.Objs() {
			err := h.client.Apply(ctx, ctrlclient.ApplyConfigurationFromUnstructured(&obj), ctrlclient.FieldOwner("dra-driver-template-e2e"))
			// API server returns 500 when webhooks are unreachable
			if errors.IsInternalError(err) {
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
				Namespace: "default",
			},
		}
		err := h.client.Delete(ctx, cluster)
		if err != nil {
			logger.Error(err, "error deleting cluster", "cluster", klog.KObj(cluster))
			return
		}
		err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
			err := h.client.Get(ctx, ctrlclient.ObjectKeyFromObject(cluster), &clusterv1.Cluster{}, &ctrlclient.GetOptions{})
			if !errors.IsNotFound(err) {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			logger.Error(err, "error waiting for cluster to be gone", "cluster", klog.KObj(cluster))
			return
		}
	})

	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true /*immediate*/, func(ctx context.Context) (bool, error) {
		cluster := new(clusterv1.Cluster)
		err := h.client.Get(ctx, ctrlclient.ObjectKey{Name: name, Namespace: "default" /*TODO*/}, cluster, &ctrlclient.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, cond := range cluster.Status.Conditions {
			if cond.Type == clusterv1.AvailableCondition &&
				cond.Status == metav1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatal("Error waiting for Clusters to become Available:", err)
	}

	kubeConfigFile, err := os.Create(filepath.Join(t.TempDir(), "kubeconfig"))
	if err != nil {
		t.Fatal("Error creating workload cluster kubeconfig:", err)
	}
	kubeConfigData, err := clusterctl.GetKubeconfig(ctx, capiclient.GetKubeconfigOptions{
		Kubeconfig:          capiclient.Kubeconfig{Path: h.kubeConfig},
		WorkloadClusterName: name,
	})
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
	client, err := ctrlclient.New(clientConfig, ctrlclient.Options{})
	if err != nil {
		t.Fatal("Error building Kubernetes client:", err)
	}

	tigeraOperator := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tigera-operator",
			Labels: map[string]string{
				"pod-security.kubernetes.io/enforce": "privileged",
				"pod-security.kubernetes.io/audit":   "privileged",
				"pod-security.kubernetes.io/warn":    "privileged",
			},
		},
	}
	err = client.Create(ctx, tigeraOperator)
	if err != nil {
		t.Fatalf("Error creating %s namespace: %v", tigeraOperator.Name, err)
	}

	helmEnv := cli.New()
	helmConfig := action.NewConfiguration(action.ConfigurationSetLogger(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug})))
	helmConfigFlags := genericclioptions.NewConfigFlags(false)
	helmConfigFlags.KubeConfig = new(kubeConfigFile.Name())
	helmConfig.Init(helmConfigFlags, tigeraOperator.Name, "")

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
	_, err = calicoInstall.RunWithContext(ctx, calicoChart, nil)
	if err != nil {
		t.Fatal("Error installing Calico chart:", err)
	}

	return clusterHandle{
		kubeConfig: kubeConfigFile.Name(),
		client:     client,
	}
}
