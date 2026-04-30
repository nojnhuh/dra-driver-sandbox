package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
)

const driverName = "template.example.com"

type driver struct {
	helper *kubeletplugin.Helper
	cancel func(error)
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
func Run(ctx context.Context, clientset kubernetes.Interface, kubeletPluginDataDirectoryPath, kubeletRegistrarDirectoryPath, nodeName, podUID string) error {
	ctx, cancel := context.WithCancelCause(ctx)

	d := &driver{
		cancel: cancel,
	}

	err := os.MkdirAll(filepath.Join(kubeletPluginDataDirectoryPath, driverName), 0750)
	if err != nil {
		return fmt.Errorf("create kubelet plugin data directory path: %w", err)
	}

	d.helper, err = kubeletplugin.Start(ctx, d,
		kubeletplugin.KubeClient(clientset),
		kubeletplugin.DriverName(driverName),
		kubeletplugin.PluginDataDirectoryPath(kubeletPluginDataDirectoryPath),
		kubeletplugin.RegistrarDirectoryPath(kubeletRegistrarDirectoryPath),
		kubeletplugin.NodeName(nodeName),
		kubeletplugin.RollingUpdate(types.UID(podUID)),
	)
	if err != nil {
		return fmt.Errorf("start kubelet plugin: %w", err)
	}
	defer d.helper.Stop()

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
	runtime.HandleErrorWithContext(ctx, err, msg)
	if !errors.Is(err, kubeletplugin.ErrRecoverable) {
		d.cancel(fatalError{err})
	}
}
