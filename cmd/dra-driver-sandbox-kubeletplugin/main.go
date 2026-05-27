package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/nojnhuh/dra-driver-sandbox/internal/driver"
)

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var driverOpts driver.Options

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

			return driver.Run(ctx, clientset, driverOpts)
		},
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	clientcmd.BindOverrideFlags(configOverrides, cmd.Flags(), clientcmd.RecommendedConfigOverrideFlags("kube-"))

	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.Flags().AddGoFlagSet(klogFlags)

	driverOpts.AddFlags(cmd.Flags())

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
