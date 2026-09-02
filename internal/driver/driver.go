package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/pflag"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
)

const driverName = "sandbox.example.com"

type Options struct {
	KubeletPluginDataDirectoryPath string
	KubeletRegistrarDirectoryPath  string
	NodeName                       string
	Namespace                      string
	PodUID                         string
}

func (o *Options) AddFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.KubeletPluginDataDirectoryPath, "kubelet-plugin-data-directory-path", kubeletplugin.KubeletPluginsDir, "Path to the kubelet's plugins directory")
	flags.StringVar(&o.KubeletRegistrarDirectoryPath, "kubelet-registrar-directory-path", kubeletplugin.KubeletRegistryDir, "Path to the kubelet's plugins registry directory")
	flags.StringVar(&o.NodeName, "node-name", os.Getenv("NODE_NAME"), "Name of the Node where the driver is running")
	flags.StringVar(&o.Namespace, "namespace", os.Getenv("NAMESPACE"), "Namespace where the driver is running")
	flags.StringVar(&o.PodUID, "pod-uid", os.Getenv("POD_UID"), "UID of the Pod in which the driver is running")
}

type driver struct {
	helper    *kubeletplugin.Helper
	cancel    func(error)
	client    kubernetes.Interface
	nodeName  string
	namespace string
}

// fatalError represents an error that causes the driver to exit
// unsuccessfully.
type fatalError struct {
	error
}

// Run starts the kubelet plugin, blocking until the context is canceled. If
// initialization fails for a fatal background error occurs, then that error is
// returned. Other causes for canceling the context cause Run to return no
// error.
func Run(ctx context.Context, clientset kubernetes.Interface, opts Options) error {
	logger := klog.FromContext(ctx)
	ctx, cancel := context.WithCancelCause(ctx)

	d := &driver{
		cancel:    cancel,
		client:    clientset,
		nodeName:  opts.NodeName,
		namespace: opts.Namespace,
	}

	err := os.MkdirAll(filepath.Join(opts.KubeletPluginDataDirectoryPath, driverName), 0750)
	if err != nil {
		return fmt.Errorf("create kubelet plugin data directory path: %w", err)
	}

	d.helper, err = kubeletplugin.Start(ctx, d,
		kubeletplugin.KubeClient(clientset),
		kubeletplugin.DriverName(driverName),
		kubeletplugin.PluginDataDirectoryPath(opts.KubeletPluginDataDirectoryPath),
		kubeletplugin.RegistrarDirectoryPath(opts.KubeletRegistrarDirectoryPath),
		kubeletplugin.NodeName(opts.NodeName),
		kubeletplugin.RollingUpdate(types.UID(opts.PodUID)),
	)
	if err != nil {
		return fmt.Errorf("start kubelet plugin: %w", err)
	}
	defer d.helper.Stop()

	controller, err := startResourcesController(ctx, clientset, d.helper, d.nodeName)
	if err != nil {
		return fmt.Errorf("start driver resources controller: %w", err)
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		defer logger.Info("Controller stopped")
		controller.run(ctx)
	})

	<-ctx.Done()

	err = context.Cause(ctx)
	if _, fatal := errors.AsType[fatalError](err); fatal {
		return err
	}
	return nil
}

// PrepareResourceClaims implements [kubeletplugin.DRAPlugin].
func (d *driver) PrepareResourceClaims(ctx context.Context, claims []*resourcev1.ResourceClaim) (result map[types.UID]kubeletplugin.PrepareResult, err error) {
	logger := klog.FromContext(ctx)

	result = make(map[types.UID]kubeletplugin.PrepareResult)
	for _, claim := range claims {
		logger.V(4).Info("Preparing claim", "claim", klog.KObj(claim), "uid", claim.UID)
		result[claim.UID] = kubeletplugin.PrepareResult{}
	}
	return
}

// UnprepareResourceClaims implements [kubeletplugin.DRAPlugin].
func (d *driver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (result map[types.UID]error, err error) {
	logger := klog.FromContext(ctx)

	result = make(map[types.UID]error)
	for _, claim := range claims {
		logger.V(4).Info("Unpreparing claim", "claim", klog.KRef(claim.Namespace, claim.Name), "uid", claim.UID)
		result[claim.UID] = nil
	}
	return
}

// HandleError implements [kubeletplugin.DRAPlugin].
func (d *driver) HandleError(ctx context.Context, err error, msg string) {
	utilruntime.HandleErrorWithContext(ctx, err, msg)
	if !errors.Is(err, kubeletplugin.ErrRecoverable) {
		d.cancel(fatalError{err})
	}
}

// WatchHealthStatus implements [kubeletplugin.DRAPlugin].
func (d *driver) WatchHealthStatus(ctx context.Context, reports chan<- kubeletplugin.DeviceHealthReport) error {
	return kubeletplugin.ErrHealthNotSupported
}
