package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"github.com/nojnhuh/dra-driver-sandbox/internal/controller"
)

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}

	enableLeaderElection := true
	leaseNamespace := os.Getenv("K8S_NAMESPACE")
	leaseName := "dra-driver-sandbox-controller"
	addLeaderElectionFlags := func(flags *pflag.FlagSet) {
		flags.BoolVar(&enableLeaderElection, "enable-leader-election", enableLeaderElection, "Whether or not leader election is enabled. This should be set when more than one replica of the controller is ever expected to run.")
		flags.StringVar(&leaseNamespace, "lease-namespace", leaseNamespace, "Namespace of the lease for leader election. Defaults to the value of the K8S_NAMESPACE environment variable.")
		flags.StringVar(&leaseName, "lease-name", leaseName, "Name of the lease for leader election")
	}

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

			if !enableLeaderElection {
				logger.V(3).Info("Running without leader election")
				return controller.Run(ctx, clientset)
			}

			lock := &resourcelock.LeaseLock{
				LeaseMeta: metav1.ObjectMeta{
					Namespace: leaseNamespace,
					Name:      leaseName,
				},
				Client: clientset.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: uuid.New().String(),
				},
			}
			ctx, cancel := context.WithCancelCause(ctx)
			defer cancel(fmt.Errorf("controller was stopped"))
			lec := leaderelection.LeaderElectionConfig{
				Lock:          lock,
				LeaseDuration: 15 * time.Second,
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   2 * time.Second,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {
						logger.V(3).Info("Running with leader election")
						if err := controller.Run(ctx, clientset); err != nil {
							logger.Error(err, "Controller failed")
						}
					},
					OnStoppedLeading: func() {
						cancel(fmt.Errorf("lost leader"))
					},
				},
			}
			le, err := leaderelection.NewLeaderElector(lec)
			if err != nil {
				return fmt.Errorf("configure leader election: %w", err)
			}
			le.Run(ctx)
			return nil
		},
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	clientcmd.BindOverrideFlags(configOverrides, cmd.Flags(), clientcmd.RecommendedConfigOverrideFlags("kube-"))

	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.Flags().AddGoFlagSet(klogFlags)

	addLeaderElectionFlags(cmd.Flags())

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
