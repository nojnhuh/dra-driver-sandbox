package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/nojnhuh/dra-driver-template/internal/driver"
	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
)

var (
	kubeletPluginDataDirectoryPath string
	kubeletRegistrarDirectoryPath  string
	nodeName                       string
	podUID                         string
)

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}

	cmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			logger := klog.FromContext(ctx)
			defer klog.Flush()

			logger.Info("Starting")
			defer logger.Info("Stopping")

			kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
			config, err := kubeConfig.ClientConfig()
			if err != nil {
				return err
			}
			clientset, err := kubernetes.NewForConfig(config)
			if err != nil {
				return nil
			}

			nodeName = os.Getenv("NODE_NAME")
			podUID = os.Getenv("POD_UID")

			return driver.Run(ctx, clientset, kubeletPluginDataDirectoryPath, kubeletRegistrarDirectoryPath, nodeName, podUID)
		},
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	clientcmd.BindOverrideFlags(configOverrides, cmd.Flags(), clientcmd.RecommendedConfigOverrideFlags("kube-"))

	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.Flags().AddGoFlagSet(klogFlags)

	cmd.Flags().StringVar(&kubeletPluginDataDirectoryPath, "kubelet-plugin-data-directory-path", kubeletplugin.KubeletPluginsDir, "Path to the kubelet's plugins directory")
	cmd.Flags().StringVar(&kubeletRegistrarDirectoryPath, "kubelet-registrar-directory-path", kubeletplugin.KubeletRegistryDir, "Path to the kubelet's plugins registry directory")

	logger := klog.NewKlogr()
	ctx = klog.NewContext(ctx, logger)

	defer logger.Info("Stopped gracefully")

	if err := cmd.ExecuteContext(ctx); err != nil {
		logger.Error(err, "Execution failed")
		return 1
	}
	return 0
}

func main() {
	klog.FlushAndExit(klog.ExitFlushTimeout, run())
}
