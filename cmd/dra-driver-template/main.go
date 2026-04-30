package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func main() {
	ctx := context.Background()

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}

	cmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			logger := klog.FromContext(ctx)
			defer klog.Flush()

			logger.Info("Starting")

			kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
			config, err := kubeConfig.ClientConfig()
			if err != nil {
				return err
			}
			clientset, err := kubernetes.NewForConfig(config)
			if err != nil {
				return nil
			}

			namespaces, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("list namespaces: %w", err)
			}
			for _, namespace := range namespaces.Items {
				logger.Info("Found Namespace", "namespace", namespace.Name)
			}

			return nil
		},
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	clientcmd.BindOverrideFlags(configOverrides, cmd.Flags(), clientcmd.RecommendedConfigOverrideFlags("kube-"))

	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.Flags().AddGoFlagSet(klogFlags)

	logger := klog.NewKlogr()
	ctx = klog.NewContext(ctx, logger)

	var exit int
	if err := cmd.ExecuteContext(ctx); err != nil {
		logger.Error(err, "Executing command failed")
		exit = 1
	}
	klog.FlushAndExit(klog.ExitFlushTimeout, exit)
}
